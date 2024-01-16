import fs from 'node:fs'
import fsp from 'node:fs/promises'
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
  _omit,
} from '@naturalcycles/js-lib'
import {
  bufferReviver,
  ReadableTyped,
  transformJsonParse,
  transformSplit,
  transformToNDJson,
  writablePushToArray,
  _pipeline,
  _emptyDir,
  _ensureDir,
  dimGrey,
  yellow,
} from '@naturalcycles/nodejs-lib'
import { CommonDB, DBIncrement, DBOperation, DBPatch, queryInMemory } from '../..'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  DBTransaction,
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
    const { tx } = opt
    if (tx) {
      ;(tx as InMemoryDBTransaction).ops.push({
        type: 'saveBatch',
        table: _table,
        rows,
        opt: _omit(opt, ['tx']),
      })
      return
    }

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

  async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt: CommonDBOptions = {},
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    if (!this.data[table]) return 0
    const ids = queryInMemory(q, Object.values(this.data[table]!) as ROW[]).map(r => r.id)

    const { tx } = opt
    if (tx) {
      ;(tx as InMemoryDBTransaction).ops.push({
        type: 'deleteByIds',
        table: q.table,
        ids,
        opt: _omit(opt, ['tx']),
      })
      return ids.length
    }

    return await this.deleteByIds(q.table, ids)
  }

  async deleteByIds(_table: string, ids: string[], opt: CommonDBOptions = {}): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
    if (!this.data[table]) return 0

    const { tx } = opt
    if (tx) {
      ;(tx as InMemoryDBTransaction).ops.push({
        type: 'deleteByIds',
        table: _table,
        ids,
        opt: _omit(opt, ['tx']),
      })
      return ids.length
    }

    let count = 0
    ids.forEach(id => {
      if (!this.data[table]![id]) return
      delete this.data[table]![id]
      count++
    })

    return count
  }

  async updateByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    patch: DBPatch<ROW>,
  ): Promise<number> {
    const patchEntries = Object.entries(patch)
    if (!patchEntries.length) return 0

    // todo: can we support tx here? :thinking:

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

  async createTransaction(): Promise<DBTransaction> {
    return new InMemoryDBTransaction(this)
  }

  /**
   * Flushes all tables (all namespaces) at once.
   */
  async flushToDisk(): Promise<void> {
    _assert(this.cfg.persistenceEnabled, 'flushToDisk() called but persistenceEnabled=false')
    const { persistentStoragePath, persistZip } = this.cfg

    const started = Date.now()

    await _emptyDir(persistentStoragePath)

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

    await _ensureDir(persistentStoragePath)

    this.data = {} // empty it in the beginning!

    const files = (await fsp.readdir(persistentStoragePath)).filter(f => f.includes('.ndjson'))

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

export class InMemoryDBTransaction implements DBTransaction {
  constructor(private db: InMemoryDB) {}

  ops: DBOperation[] = []

  async commit(): Promise<void> {
    const backup = _deepCopy(this.db.data)

    try {
      for (const op of this.ops) {
        if (op.type === 'saveBatch') {
          await this.db.saveBatch(op.table, op.rows, op.opt)
        } else if (op.type === 'deleteByIds') {
          await this.db.deleteByIds(op.table, op.ids, op.opt)
        } else {
          throw new Error(`DBOperation not supported: ${(op as any).type}`)
        }
      }

      this.ops = []
    } catch (err) {
      // rollback
      this.db.data = backup
      this.db.cfg.logger!.log('InMemoryDB transaction rolled back')

      throw err
    }
  }

  async rollback(): Promise<void> {
    this.ops = []
  }
}
