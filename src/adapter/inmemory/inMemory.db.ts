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
  PartialObjectWithId,
  Saved,
} from '@naturalcycles/js-lib'
import {
  bufferReviver,
  ReadableTyped,
  transformJsonParse,
  transformSplit,
  transformToNDJson,
  writablePushToArray,
  _pipeline,
  dimGrey,
  yellow,
  fs2,
} from '@naturalcycles/nodejs-lib'
import {
  CommonDB,
  commonDBFullSupport,
  CommonDBTransactionOptions,
  CommonDBType,
  DBIncrement,
  DBOperation,
  DBPatch,
  DBTransactionFn,
  queryInMemory,
} from '../..'
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
   * Many DB implementations (e.g Datastore and Firestore) forbid doing
   * read operations after a write/delete operation was done inside a Transaction.
   *
   * To help spot that type of bug - InMemoryDB by default has this setting to `true`,
   * which will throw on such occasions.
   *
   * Defaults to true.
   */
  forbidTransactionReadAfterWrite?: boolean

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
  dbType = CommonDBType.document

  support = {
    ...commonDBFullSupport,
  }

  constructor(cfg?: Partial<InMemoryDBCfg>) {
    this.cfg = {
      // defaults
      tablesPrefix: '',
      forbidTransactionReadAfterWrite: true,
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

  async getTableSchema<ROW extends PartialObjectWithId>(
    _table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    const table = this.cfg.tablesPrefix + _table
    return {
      ...generateJsonSchemaFromData(_stringMapValues(this.data[table] || {})),
      $id: `${table}.schema.json`,
    }
  }

  async createTable<ROW extends PartialObjectWithId>(
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

  async getByIds<ROW extends PartialObjectWithId>(
    _table: string,
    ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<Saved<ROW>[]> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] ||= {}
    return ids.map(id => this.data[table]![id] as Saved<ROW>).filter(Boolean)
  }

  async saveBatch<ROW extends PartialObjectWithId>(
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

  async deleteByQuery<ROW extends PartialObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    if (!this.data[table]) return 0
    const ids = queryInMemory(q, Object.values(this.data[table]!) as Saved<ROW>[]).map(r => r.id)
    return await this.deleteByIds(q.table, ids)
  }

  async deleteByIds(_table: string, ids: string[], _opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
    if (!this.data[table]) return 0

    let count = 0
    ids.forEach(id => {
      if (!this.data[table]![id]) return
      delete this.data[table]![id]
      count++
    })

    return count
  }

  async updateByQuery<ROW extends PartialObjectWithId>(
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

  async runQuery<ROW extends PartialObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<RunQueryResult<Saved<ROW>>> {
    const table = this.cfg.tablesPrefix + q.table
    return { rows: queryInMemory(q, Object.values(this.data[table] || {}) as Saved<ROW>[]) }
  }

  async runQueryCount<ROW extends PartialObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    return queryInMemory<any>(q, Object.values(this.data[table] || {})).length
  }

  streamQuery<ROW extends PartialObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): ReadableTyped<Saved<ROW>> {
    const table = this.cfg.tablesPrefix + q.table
    return Readable.from(queryInMemory(q, Object.values(this.data[table] || {}) as ROW[]))
  }

  async runInTransaction(fn: DBTransactionFn, opt: CommonDBTransactionOptions = {}): Promise<void> {
    const tx = new InMemoryDBTransaction(this, {
      readOnly: false,
      ...opt,
    })

    try {
      await fn(tx)
      await tx.commit()
    } catch (err) {
      await tx.rollback()
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

    await fs2.emptyDirAsync(persistentStoragePath)

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

    await fs2.ensureDirAsync(persistentStoragePath)

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
  constructor(
    private db: InMemoryDB,
    private opt: Required<CommonDBTransactionOptions>,
  ) {}

  ops: DBOperation[] = []

  // used to enforce forbidReadAfterWrite setting
  writeOperationHappened = false

  async getByIds<ROW extends PartialObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    if (this.db.cfg.forbidTransactionReadAfterWrite) {
      _assert(
        !this.writeOperationHappened,
        `InMemoryDBTransaction: read operation attempted after write operation`,
      )
    }

    return await this.db.getByIds(table, ids, opt)
  }

  async saveBatch<ROW extends PartialObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions<ROW>,
  ): Promise<void> {
    _assert(
      !this.opt.readOnly,
      `InMemoryDBTransaction: saveBatch(${table}) called in readOnly mode`,
    )

    this.writeOperationHappened = true

    this.ops.push({
      type: 'saveBatch',
      table,
      rows,
      opt,
    })
  }

  async deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    _assert(
      !this.opt.readOnly,
      `InMemoryDBTransaction: deleteByIds(${table}) called in readOnly mode`,
    )

    this.writeOperationHappened = true

    this.ops.push({
      type: 'deleteByIds',
      table,
      ids,
      opt,
    })
    return ids.length
  }

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
      this.ops = []
      this.db.data = backup
      this.db.cfg.logger!.log('InMemoryDB transaction rolled back')

      throw err
    }
  }

  async rollback(): Promise<void> {
    this.ops = []
  }
}
