import { Readable } from 'stream'
import { createGzip, createUnzip } from 'zlib'
import {
  generateJsonSchemaFromData,
  JsonSchemaObject,
  pMap,
  StringMap,
  _by,
  _since,
  _sortObjectDeep,
  JsonSchemaRootObject,
} from '@naturalcycles/js-lib'
import {
  bufferReviver,
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
import { CommonDB, DBTransaction, ObjectWithId, queryInMemory } from '../..'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  RunQueryResult,
} from '../../db.model'
import { DBQuery } from '../../query/dbQuery'

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
  data: StringMap<StringMap<ObjectWithId>> = {}

  /**
   * Returns internal "Data snapshot".
   * Deterministic - jsonSorted.
   */
  getDataSnapshot(): StringMap<StringMap<ObjectWithId>> {
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

  async getTableSchema<ROW extends ObjectWithId>(
    _table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    const table = this.cfg.tablesPrefix + _table
    return {
      ...generateJsonSchemaFromData(Object.values(this.data[table] || {})),
      $id: `${table}.schema.json`,
    }
  }

  async createTable(
    _table: string,
    _schema: JsonSchemaObject,
    opt: CommonDBCreateOptions = {},
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    if (opt.dropIfExists) {
      this.data[table] = {}
    } else {
      this.data[table] = this.data[table] || {}
    }
  }

  async getByIds<ROW extends ObjectWithId>(
    _table: string,
    ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}
    return ids.map(id => this.data[table]![id]).filter(Boolean) as ROW[]
  }

  async saveBatch<ROW extends ObjectWithId>(
    _table: string,
    rows: ROW[],
    _opt?: CommonDBSaveOptions,
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}

    rows.forEach(r => {
      if (!r.id) {
        log.warn({ rows })
        throw new Error(`InMemoryDB: id doesn't exist for row`)
      }

      // JSON parse/stringify (deep clone) is to:
      // 1. Not store values "by reference" (avoid mutation bugs)
      // 2. Simulate real DB that would do something like that in a transport layer anyway
      this.data[table]![r.id] = JSON.parse(JSON.stringify(r), bufferReviver)

      // special treatment for Buffers (assign them raw, without JSON parse/stringify)
      // Object.entries(r).forEach(([k, v]) => {
      //   if (Buffer.isBuffer(v)) {
      //     this.data[table]![r.id]![k] = v
      //   }
      // })
    })
  }

  async deleteByIds(_table: string, ids: string[], _opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}

    return ids
      .map(id => {
        const exists = !!this.data[table]![id]
        delete this.data[table]![id]
        if (exists) return id
      })
      .filter(Boolean).length
  }

  async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    const rows = queryInMemory(q, Object.values(this.data[table] || {}) as ROW[])
    const ids = rows.map(r => r.id)
    return await this.deleteByIds(q.table, ids)
  }

  async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    const table = this.cfg.tablesPrefix + q.table
    return { rows: queryInMemory(q, Object.values(this.data[table] || {}) as ROW[]) }
  }

  async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    return queryInMemory<any>(q, Object.values(this.data[table] || {})).length
  }

  streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): ReadableTyped<ROW> {
    const table = this.cfg.tablesPrefix + q.table
    return Readable.from(queryInMemory(q, Object.values(this.data[table] || {}) as ROW[]))
  }

  async commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void> {
    for await (const op of tx.ops) {
      if (op.type === 'saveBatch') {
        await this.saveBatch(op.table, op.rows, opt)
      } else if (op.type === 'deleteByIds') {
        await this.deleteByIds(op.table, op.ids, opt)
      } else {
        throw new Error(`DBOperation not supported: ${(op as any).type}`)
      }
    }
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
      const rows = Object.values(this.data[table]!)
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
      const table = file.split('.ndjson')[0]!

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
