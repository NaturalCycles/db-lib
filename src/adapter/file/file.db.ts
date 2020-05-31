import { _by, _deepEquals, _since, _sortBy, _sortObjectDeep } from '@naturalcycles/js-lib'
import { Debug, readableCreate, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { DBSaveBatchOperation, ObjectWithId, queryInMemory } from '../..'
import { CommonSchema } from '../..'
import { CommonSchemaGenerator } from '../..'
import { CommonDB } from '../../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  RunQueryResult,
} from '../../db.model'
import { DBQuery } from '../../query/dbQuery'
import { FileDBCfg } from './file.db.model'
import { FileDBTransaction } from './fileDBTransaction'

const log = Debug('nc:db-lib:filedb')

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
export class FileDB implements CommonDB {
  constructor(cfg: FileDBCfg) {
    this.cfg = {
      sortObjects: true,
      logFinished: true,
      ...cfg,
    }
  }

  public cfg!: FileDBCfg

  async ping(): Promise<void> {
    await this.cfg.plugin.ping()
  }

  async getTables(): Promise<string[]> {
    const started = this.logStarted('getTables()')
    const tables = await this.cfg.plugin.getTables()
    this.logFinished(started, `getTables() ${tables.length} tables`)
    return tables
  }

  async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    const byId = _by(await this.loadFile<ROW>(table), r => r.id)
    return ids.map(id => byId[id]).filter(Boolean)
  }

  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!rows.length) return // save some api calls

    // 1. Load the whole file from gh
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
      await this.saveFile(table, Object.values(byId))
    }
  }

  async runQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    return {
      rows: queryInMemory(q, await this.loadFile<ROW>(q.table)),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return (await this.loadFile(q.table)).length
  }

  streamQuery<ROW extends ObjectWithId, OUT = ROW>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT> {
    const readable = readableCreate<ROW>()

    void this.runQuery(q, opt).then(({ rows }) => {
      rows.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  async deleteByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
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

  async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const byId = _by(await this.loadFile<ROW>(q.table), r => r.id)

    let deleted = 0
    queryInMemory(q, Object.values(byId)).forEach(r => {
      delete byId[r.id]
      deleted++
    })

    if (deleted > 0) {
      await this.saveFile(q.table, Object.values(byId))
    }

    return deleted
  }

  transaction(): FileDBTransaction {
    return new FileDBTransaction(this)
  }

  // no-op
  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  async getTableSchema<ROW extends ObjectWithId>(table: string): Promise<CommonSchema<ROW>> {
    const rows = await this.loadFile(table)
    return CommonSchemaGenerator.generateFromRows({ table }, rows)
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
    // Sort the rows, if needed
    const rows = this.sortRows(_rows)

    const op = `saveFile(${table}) ${rows.length} row(s)`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles([{ type: 'saveBatch', table, rows }])
    this.logFinished(started, op)
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {
    const op = `saveFiles ${ops.length} op(s):\n${ops
      .map(o => `${o.table} (${o.rows.length})`)
      .join('\n')}`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles(ops)
    this.logFinished(started, op)
  }

  /**
   * Mutates
   */
  sortRows<ROW>(rows: ROW[]): ROW[] {
    if (this.cfg.sortOnSave) {
      _sortBy(rows, this.cfg.sortOnSave.name, true)
      if (this.cfg.sortOnSave.descending) rows.reverse() // mutates
    }

    if (this.cfg.sortObjects) {
      return _sortObjectDeep(rows)
    }

    return rows
  }

  private logStarted(op: string): number {
    if (this.cfg.logStarted) {
      log(`>> ${op}`)
    }
    return Date.now()
  }

  private logFinished(started: number, op: string): void {
    if (!this.cfg.logFinished) return
    log(`<< ${op} in ${_since(started)}`)
  }
}
