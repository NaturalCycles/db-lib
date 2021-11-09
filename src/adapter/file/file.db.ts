import {
  generateJsonSchemaFromData,
  pMap,
  StringMap,
  _by,
  _deepEquals,
  _since,
  _sortBy,
  _sortObjectDeep,
  _stringMapValues,
  _uniq,
  JsonSchemaRootObject,
  _filterUndefinedValues,
  ObjectWithId,
  AnyObjectWithId,
} from '@naturalcycles/js-lib'
import { readableCreate, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { dimGrey } from '@naturalcycles/nodejs-lib/dist/colors'
import { BaseCommonDB, DBSaveBatchOperation, queryInMemory } from '../..'
import { CommonDB } from '../../common.db'
import {
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  RunQueryResult,
} from '../../db.model'
import { DBQuery } from '../../query/dbQuery'
import { DBTransaction } from '../../transaction/dbTransaction'
import { FileDBCfg } from './file.db.model'

/**
 * Provides barebone implementation for "whole file" based CommonDB.
 * "whole file" means that the persistence layer doesn't allow any querying,
 * but allows to read the whole file or save the whole file.
 * For example, Google Cloud Storage / S3 that store ndjson files will be such persistence.
 *
 * In contrast with InMemoryDB, FileDB stores *nothing* in memory.
 * Each load/query operation loads *whole* file from the persitence layer.
 * Each save operation saves *whole* file to the persistence layer.
 */
export class FileDB extends BaseCommonDB implements CommonDB {
  constructor(cfg: FileDBCfg) {
    super()
    this.cfg = {
      sortObjects: true,
      logFinished: true,
      logger: console,
      ...cfg,
    }
  }

  public cfg!: FileDBCfg

  override async ping(): Promise<void> {
    await this.cfg.plugin.ping()
  }

  override async getTables(): Promise<string[]> {
    const started = this.logStarted('getTables()')
    const tables = await this.cfg.plugin.getTables()
    this.logFinished(started, `getTables() ${tables.length} tables`)
    return tables
  }

  override async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    const byId = _by(await this.loadFile<ROW>(table), r => r.id)
    return ids.map(id => byId[id]!).filter(Boolean)
  }

  override async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    _opt?: CommonDBSaveOptions<ROW>,
  ): Promise<void> {
    if (!rows.length) return // save some api calls

    // 1. Load the whole file
    const byId = _by(await this.loadFile<ROW>(table), r => r.id)

    // 2. Merge with new data (using ids)
    let saved = 0
    rows.forEach(r => {
      if (!_deepEquals(byId[r.id], r)) {
        byId[r.id] = r
        saved++
      }
    })

    // Only save if there are changed rows
    if (saved > 0) {
      // 3. Save the whole file
      await this.saveFile(table, _stringMapValues(byId))
    }
  }

  /**
   * Implementation is optimized for loading/saving _whole files_.
   */
  override async commitTransaction(tx: DBTransaction, _opt?: CommonDBOptions): Promise<void> {
    // data[table][id] => row
    const data: StringMap<StringMap<ObjectWithId>> = {}

    // 1. Load all tables data (concurrently)
    const tables = _uniq(tx.ops.map(o => o.table))

    await pMap(
      tables,
      async table => {
        const rows = await this.loadFile(table)
        data[table] = _by(rows, r => r.id)
      },
      { concurrency: 16 },
    )

    // 2. Apply ops one by one (in order)
    tx.ops.forEach(op => {
      if (op.type === 'deleteByIds') {
        op.ids.forEach(id => delete data[op.table]![id])
      } else if (op.type === 'saveBatch') {
        op.rows.forEach(r => (data[op.table]![r.id] = r))
      } else {
        throw new Error(`DBOperation not supported: ${(op as any).type}`)
      }
    })

    // 3. Sort, turn it into ops
    // Not filtering empty arrays, cause it's already filtered in this.saveFiles()
    const ops: DBSaveBatchOperation[] = Object.keys(data).map(table => {
      return {
        type: 'saveBatch',
        table,
        rows: this.sortRows(Object.values(data[table]!) as AnyObjectWithId[]),
      }
    })

    // 4. Save all files
    await this.saveFiles(ops)
  }

  override async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    return {
      rows: queryInMemory(q, await this.loadFile<ROW>(q.table)),
    }
  }

  override async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    return (await this.loadFile(q.table)).length
  }

  override streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW> {
    const readable = readableCreate<ROW>()

    void this.runQuery(q, opt).then(({ rows }) => {
      rows.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  override async deleteByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    _opt?: CommonDBOptions,
  ): Promise<number> {
    if (!ids.length) return 0

    let deleted = 0
    const rows = (await this.loadFile<ROW>(table)).filter(r => {
      if (ids.includes(r.id)) {
        deleted++
        return false
      }
      return true
    })

    if (deleted > 0) {
      await this.saveFile(table, rows)
    }

    return deleted
  }

  override async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    const byId = _by(await this.loadFile<ROW>(q.table), r => r.id)

    let deleted = 0
    queryInMemory(q, _stringMapValues(byId)).forEach(r => {
      delete byId[r.id]
      deleted++
    })

    if (deleted > 0) {
      await this.saveFile(q.table, _stringMapValues(byId))
    }

    return deleted
  }

  override async getTableSchema<ROW extends ObjectWithId>(
    table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    const rows = await this.loadFile(table)
    return {
      ...generateJsonSchemaFromData(rows),
      $id: `${table}.schema.json`,
    }
  }

  // wrapper, to handle logging
  async loadFile<ROW extends ObjectWithId>(table: string): Promise<ROW[]> {
    const started = this.logStarted(`loadFile(${table})`)
    const rows = await this.cfg.plugin.loadFile<ROW>(table)
    this.logFinished(started, `loadFile(${table}) ${rows.length} row(s)`)
    return rows
  }

  // wrapper, to handle logging, sorting rows before saving
  async saveFile<ROW extends ObjectWithId>(table: string, _rows: ROW[]): Promise<void> {
    // if (!_rows.length) return // NO, it should be able to save file with 0 rows!

    // Sort the rows, if needed
    const rows = this.sortRows(_rows)

    const op = `saveFile(${table}) ${rows.length} row(s)`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles([{ type: 'saveBatch', table, rows }])
    this.logFinished(started, op)
  }

  async saveFiles<ROW extends ObjectWithId>(ops: DBSaveBatchOperation<ROW>[]): Promise<void> {
    if (!ops.length) return
    const op =
      `saveFiles ${ops.length} op(s):\n` + ops.map(o => `${o.table} (${o.rows.length})`).join('\n')
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles(ops)
    this.logFinished(started, op)
  }

  sortRows<ROW>(rows: ROW[]): ROW[] {
    rows = rows.map(r => _filterUndefinedValues(r))

    if (this.cfg.sortOnSave) {
      _sortBy(rows, r => r[this.cfg.sortOnSave!.name], true)
      if (this.cfg.sortOnSave.descending) rows.reverse() // mutates
    }

    if (this.cfg.sortObjects) {
      return _sortObjectDeep(rows)
    }

    return rows
  }

  private logStarted(op: string): number {
    if (this.cfg.logStarted) {
      this.cfg.logger?.log(`>> ${op}`)
    }
    return Date.now()
  }

  private logFinished(started: number, op: string): void {
    if (!this.cfg.logFinished) return
    this.cfg.logger?.log(`<< ${op} ${dimGrey(`in ${_since(started)}`)}`)
  }
}
