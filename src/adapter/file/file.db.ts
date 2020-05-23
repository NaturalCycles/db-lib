import { _by, _deepEquals, _since, _sortBy, _sortObjectDeep } from '@naturalcycles/js-lib'
import { Debug, readableCreate, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { queryInMemory } from '../..'
import { CommonSchema } from '../..'
import { CommonSchemaGenerator } from '../..'
import { CommonDB } from '../../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  RunQueryResult,
  SavedDBEntity,
} from '../../db.model'
import { DBQuery } from '../../dbQuery'
import { FileDBCfg } from './file.db.model'

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

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const byId = _by(await this.loadFile<DBM>(table), r => r.id)
    return ids.map(id => byId[id]).filter(Boolean)
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {
    if (!dbms.length) return // save some api calls

    // 1. Load the whole file from gh
    const byId = _by(await this.loadFile<DBM>(table), r => r.id)

    // 2. Merge with new data (using ids)
    let saved = 0
    dbms.forEach(dbm => {
      if (!_deepEquals(byId[dbm.id], dbm)) {
        byId[dbm.id] = dbm
        saved++
      }
    })

    // Only save if there are changed rows
    if (saved > 0) {
      // 3. Save the whole file
      await this.saveFile(table, Object.values(byId))
    }
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    return {
      records: queryInMemory(q, await this.loadFile<DBM>(q.table)),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return (await this.loadFile(q.table)).length
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT> {
    const readable = readableCreate<DBM>()

    void this.runQuery(q, opt).then(({ records }) => {
      records.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  async deleteByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<number> {
    if (!ids.length) return 0

    let deleted = 0
    const dbms = (await this.loadFile<DBM>(table)).filter(dbm => {
      if (ids.includes(dbm.id)) {
        deleted++
        return false
      }
      return true
    })

    if (deleted > 0) {
      await this.saveFile(table, dbms)
    }

    return deleted
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const byId = _by(await this.loadFile<DBM>(q.table), r => r.id)

    let deleted = 0
    queryInMemory(q, Object.values(byId)).forEach(dbm => {
      delete byId[dbm.id]
      deleted++
    })

    if (deleted > 0) {
      await this.saveFile(q.table, Object.values(byId))
    }

    return deleted
  }

  // no-op
  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  // no-op
  async resetCache(table?: string): Promise<void> {}

  async getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>> {
    const dbms = await this.loadFile(table)
    return CommonSchemaGenerator.generateFromRows({ table }, dbms)
  }

  // wrapper, to handle logging
  private async loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    const started = this.logStarted(`loadFile(${table})`)
    const dbms = await this.cfg.plugin.loadFile<DBM>(table)
    this.logFinished(started, `loadFile(${table}) ${dbms.length} records`)
    return dbms
  }

  // wrapper, to handle logging, sorting rows before saving
  private async saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void> {
    // Sort the records, if needed
    const rows = this.sortDBMs(dbms)

    const op = `saveFile(${table}) ${rows.length} records`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFile<DBM>(table, rows)
    this.logFinished(started, op)
  }

  /**
   * Mutates
   */
  private sortDBMs<DBM>(dbms: DBM[]): DBM[] {
    if (this.cfg.sortOnSave) {
      _sortBy(dbms, this.cfg.sortOnSave.name, true)
      if (this.cfg.sortOnSave.descending) dbms.reverse() // mutates
    }

    if (this.cfg.sortObjects) {
      return _sortObjectDeep(dbms)
    }

    return dbms
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
