import { Readable } from 'node:stream'
import { createGzip, createUnzip } from 'node:zlib'
import {
  generateJsonSchemaFromData,
  JsonSchemaObject,
  pMap,
  StringMap,
  _by,
  _since,
  _sortObjectDeep,
  JsonSchemaRootObject,
  ObjectWithId,
  _stringMapValues,
  CommonLogger,
  _deepCopy,
  _assert,
} from '@naturalcycles/js-lib'
import {
  bufferReviver,
  ReadableTyped,
  transformJsonParse,
  transformSplit,
  transformToNDJson,
  writablePushToArray,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import { dimGrey, yellow } from '@naturalcycles/nodejs-lib/dist/colors'
import * as fs from 'fs-extra'
import { CommonDB, DBIncrement, DBPatch, DBTransaction, queryInMemory } from '../..'
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

  /**
   * Defaults to `console`.
   */
  logger?: CommonLogger
}

export class InMemoryDB implements CommonDB {
  constructor(cfg?: Partial<InMemoryDBCfg>) {
    this.cfg = {
      // defaults
      tablesPrefix: '',
      persistenceEnabled: false,
      persistZip: true,
      persistentStoragePath: './tmp/inmemorydb',
      logger: console,
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
      this.cfg.logger!.log(`reset ${table}`)
      this.data[table] = {}
    } else {
      ;(await this.getTables()).forEach(table => {
        this.data[table] = {}
      })
      this.cfg.logger!.log('reset')
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
      ...generateJsonSchemaFromData(_stringMapValues(this.data[table] || {})),
      $id: `${table}.schema.json`,
    }
  }

  async createTable<ROW extends ObjectWithId>(
    _table: string,
    _schema: JsonSchemaObject<ROW>,
    opt: CommonDBCreateOptions = {},
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    if (opt.dropIfExists) {
      this.data[table] = {}
    } else {
      this.data[table] ||= {}
    }
  }

  async getByIds<ROW extends ObjectWithId>(
    _table: string,
    ids: ROW['id'][],
    _opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] ||= {}
    return ids.map(id => this.data[table]![id] as ROW).filter(Boolean)
  }

  async saveBatch<ROW extends Partial<ObjectWithId>>(
    _table: string,
    rows: ROW[],
    opt: CommonDBSaveOptions<ROW> = {},
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] ||= {}

    rows.forEach(r => {
      if (!r.id) {
        this.cfg.logger!.warn({ rows })
        throw new Error(
          `InMemoryDB doesn't support id auto-generation in saveBatch, row without id was given`,
        )
      }

      if (opt.saveMethod === 'insert' && this.data[table]![r.id]) {
        throw new Error(`InMemoryDB: INSERT failed, entity exists: ${table}.${r.id}`)
      }

      if (opt.saveMethod === 'update' && !this.data[table]![r.id]) {
        throw new Error(`InMemoryDB: UPDATE failed, entity doesn't exist: ${table}.${r.id}`)
      }

      // JSON parse/stringify (deep clone) is to:
      // 1. Not store values "by reference" (avoid mutation bugs)
      // 2. Simulate real DB that would do something like that in a transport layer anyway
      this.data[table]![r.id] = JSON.parse(JSON.stringify(r), bufferReviver)
    })
  }

  async deleteByIds<ROW extends ObjectWithId>(
    _table: string,
    ids: ROW['id'][],
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] ||= {}
    let count = 0
    ids.forEach(id => {
      if (!this.data[table]![id]) return
      delete this.data[table]![id]
      count++
    })
    return count
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

  async updateByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    patch: DBPatch<ROW>,
  ): Promise<number> {
    const patchEntries = Object.entries(patch)
    if (!patchEntries.length) return 0

    const table = this.cfg.tablesPrefix + q.table
    const rows = queryInMemory(q, Object.values(this.data[table] || {}) as ROW[])
    rows.forEach((row: any) => {
      patchEntries.forEach(([k, v]) => {
        if (v instanceof DBIncrement) {
          row[k] = (row[k] || 0) + v.amount
        } else {
          row[k] = v
        }
      })
    })

    return rows.length
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

  async commitTransaction(tx: DBTransaction, opt?: CommonDBOptions): Promise<void> {
    const backup = _deepCopy(this.data)

    try {
      for await (const op of tx.ops) {
        if (op.type === 'saveBatch') {
          await this.saveBatch(op.table, op.rows, { ...op.opt, ...opt })
        } else if (op.type === 'deleteByIds') {
          await this.deleteByIds(op.table, op.ids, { ...op.opt, ...opt })
        } else {
          throw new Error(`DBOperation not supported: ${(op as any).type}`)
        }
      }
    } catch (err) {
      // rollback
      this.data = backup
      this.cfg.logger!.log('InMemoryDB transaction rolled back')

      throw err
    }
  }

  /**
   * Flushes all tables (all namespaces) at once.
   */
  async flushToDisk(): Promise<void> {
    _assert(this.cfg.persistenceEnabled, 'flushToDisk() called but persistenceEnabled=false')
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

    this.cfg.logger!.log(
      `flushToDisk took ${dimGrey(_since(started))} to save ${yellow(tables)} tables`,
    )
  }

  /**
   * Restores all tables (all namespaces) at once.
   */
  async restoreFromDisk(): Promise<void> {
    _assert(this.cfg.persistenceEnabled, 'restoreFromDisk() called but persistenceEnabled=false')
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

    this.cfg.logger!.log(
      `restoreFromDisk took ${dimGrey(_since(started))} to read ${yellow(files.length)} tables`,
    )
  }
}
