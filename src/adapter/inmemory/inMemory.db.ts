import { pMap, StringMap, _by, _since, _sortObjectDeep } from '@naturalcycles/js-lib'
import {
  Debug,
  ReadableTyped,
  transformJsonParse,
  transformSplit,
  transformToNDJson,
  writablePushToArray,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import { dimGrey, yellow } from '@naturalcycles/nodejs-lib/dist/colors'
import * as fs from 'fs-extra'
import { Readable } from 'stream'
import { createGzip, createUnzip } from 'zlib'
import { CommonDB, queryInMemory } from '../..'
import { CommonSchema } from '../..'
import { CommonSchemaGenerator } from '../..'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  RunQueryResult,
  SavedDBEntity,
} from '../../db.model'
import { DBQuery } from '../../dbQuery'

export interface InMemoryDBCfg {
  /**
   * @default ''
   *
   * Allows to support "Namespacing".
   * E.g, pass `ns1_` to it and all tables will be prefixed by it.
   * Reset cache respects this prefix (won't touch other namespaces!)
   */
  tablesPrefix: string

  /**
   * @default false
   *
   * Set to true to enable disk persistence (!).
   */
  persistenceEnabled: boolean

  /**
   * @default ./tmp/inmemorydb
   *
   * Will store one ndjson file per table.
   * Will only flush on demand (see .flushToDisk() and .restoreFromDisk() methods).
   * Even if persistence is enabled - nothing is flushed or restored automatically.
   */
  persistentStoragePath: string

  /**
   * @default true
   */
  persistZip: boolean
}

const log = Debug('nc:db-lib:inmemorydb')

export class InMemoryDB implements CommonDB {
  constructor(cfg?: Partial<InMemoryDBCfg>) {
    this.cfg = {
      // defaults
      tablesPrefix: '',
      persistenceEnabled: false,
      persistZip: true,
      persistentStoragePath: './tmp/inmemorydb',
      ...cfg,
    }
  }

  cfg: InMemoryDBCfg

  // data[table][id] > {id: 'a', created: ... }
  data: StringMap<StringMap<SavedDBEntity>> = {}

  /**
   * Returns internal "Data snapshot".
   * Deterministic - jsonSorted.
   */
  getDataSnapshot(): StringMap<StringMap<SavedDBEntity>> {
    return _sortObjectDeep(this.data)
  }

  async ping(): Promise<void> {}

  /**
   * Resets InMemory DB data
   */
  async resetCache(_table?: string): Promise<void> {
    if (_table) {
      const table = this.cfg.tablesPrefix + _table
      log(`reset ${table}`)
      this.data[table] = {}
    } else {
      ;(await this.getTables()).forEach(table => {
        this.data[table] = {}
      })
      log('reset')
    }
  }

  async getTables(): Promise<string[]> {
    return Object.keys(this.data).filter(t => t.startsWith(this.cfg.tablesPrefix))
  }

  async getTableSchema<DBM extends SavedDBEntity>(_table: string): Promise<CommonSchema<DBM>> {
    const table = this.cfg.tablesPrefix + _table
    return CommonSchemaGenerator.generateFromRows({ table }, Object.values(this.data[table] || {}))
  }

  async createTable(schema: CommonSchema, opt: CommonDBCreateOptions = {}): Promise<void> {
    const table = this.cfg.tablesPrefix + schema.table
    if (opt.dropIfExists) {
      this.data[table] = {}
    } else {
      this.data[table] = this.data[table] || {}
    }
  }

  async getByIds<DBM extends SavedDBEntity>(
    _table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}
    return ids.map(id => this.data[table][id]).filter(Boolean) as DBM[]
  }

  async saveBatch<DBM extends SavedDBEntity>(
    _table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}

    dbms.forEach(dbm => {
      if (!dbm.id) {
        log.warn({ dbms })
        throw new Error(`InMemoryDB: id doesn't exist for record`)
      }
      this.data[table][dbm.id] = dbm
    })
  }

  async deleteByIds(_table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}

    return ids
      .map(id => {
        const exists = !!this.data[table][id]
        delete this.data[table][id]
        if (exists) return id
      })
      .filter(Boolean).length
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    const rows = queryInMemory(q, Object.values(this.data[table] || {}) as DBM[])
    const ids = rows.map(r => r.id)
    return this.deleteByIds(q.table, ids)
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const table = this.cfg.tablesPrefix + q.table
    return { records: queryInMemory<DBM, OUT>(q, Object.values(this.data[table] || {}) as DBM[]) }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    return queryInMemory(q, Object.values(this.data[table] || {})).length
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): ReadableTyped<OUT> {
    const table = this.cfg.tablesPrefix + q.table
    return Readable.from(queryInMemory<DBM, OUT>(q, Object.values(this.data[table] || {}) as DBM[]))
  }

  /**
   * Flushes all tables (all namespaces) at once.
   */
  async flushToDisk(): Promise<void> {
    if (!this.cfg.persistenceEnabled) {
      throw new Error('flushToDisk() called but persistenceEnabled=false')
    }
    const { persistentStoragePath, persistZip } = this.cfg

    const started = Date.now()

    await fs.emptyDir(persistentStoragePath)

    const transformZip = persistZip ? [createGzip()] : []
    let tables = 0

    // infinite concurrency for now
    await pMap(Object.keys(this.data), async table => {
      const rows = Object.values(this.data[table])
      if (rows.length === 0) return // 0 rows

      tables++
      const fname = `${persistentStoragePath}/${table}.ndjson${persistZip ? '.gz' : ''}`

      await _pipeline([
        Readable.from(rows),
        transformToNDJson(),
        ...transformZip,
        fs.createWriteStream(fname),
      ])
    })

    log(`flushToDisk took ${dimGrey(_since(started))} to save ${yellow(tables)} tables`)
  }

  /**
   * Restores all tables (all namespaces) at once.
   */
  async restoreFromDisk(): Promise<void> {
    if (!this.cfg.persistentStoragePath) {
      throw new Error('restoreFromDisk() called but persistenceEnabled=false')
    }
    const { persistentStoragePath } = this.cfg

    const started = Date.now()

    await fs.ensureDir(persistentStoragePath)

    this.data = {} // empty it in the beginning!

    const files = (await fs.readdir(persistentStoragePath)).filter(f => f.includes('.ndjson'))

    // infinite concurrency for now
    await pMap(files, async file => {
      const fname = `${persistentStoragePath}/${file}`
      const [table] = file.split('.ndjson')

      const transformUnzip = file.endsWith('.gz') ? [createUnzip()] : []

      const rows: any[] = []

      await _pipeline([
        fs.createReadStream(fname),
        ...transformUnzip,
        transformSplit(), // splits by \n
        transformJsonParse(),
        writablePushToArray(rows),
      ])

      this.data[table] = _by(rows, r => r.id)
    })

    log(`restoreFromDisk took ${dimGrey(_since(started))} to read ${yellow(files.length)} tables`)
  }
}
