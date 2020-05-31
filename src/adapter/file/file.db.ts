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

  async getByIds<DBM extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const byId = _by(await this.loadFile<DBM>(table), r => r.id)
    return ids.map(id => byId[id]).filter(Boolean)
  }

  async saveBatch<DBM extends ObjectWithId>(
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

  async runQuery<DBM extends ObjectWithId, OUT = DBM>(
    q: DBQuery<DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    return {
      records: queryInMemory(q, await this.loadFile<DBM>(q.table)),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return (await this.loadFile(q.table)).length
  }

  streamQuery<DBM extends ObjectWithId, OUT = DBM>(
    q: DBQuery<DBM>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT> {
    const readable = readableCreate<DBM>()

    void this.runQuery(q, opt).then(({ records }) => {
      records.forEach(r => readable.push(r))
      readable.push(null) // done
    })

    return readable
  }

  async deleteByIds<DBM extends ObjectWithId>(
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

  async deleteByQuery<DBM extends ObjectWithId>(
    q: DBQuery<DBM>,
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

  transaction(): FileDBTransaction {
    return new FileDBTransaction(this)
  }

  // no-op
  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  // no-op
  async resetCache(table?: string): Promise<void> {}

  async getTableSchema<DBM extends ObjectWithId>(table: string): Promise<CommonSchema<DBM>> {
    const dbms = await this.loadFile(table)
    return CommonSchemaGenerator.generateFromRows({ table }, dbms)
  }

  // wrapper, to handle logging
  async loadFile<DBM extends ObjectWithId>(table: string): Promise<DBM[]> {
    const started = this.logStarted(`loadFile(${table})`)
    const dbms = await this.cfg.plugin.loadFile<DBM>(table)
    this.logFinished(started, `loadFile(${table}) ${dbms.length} record(s)`)
    return dbms
  }

  // wrapper, to handle logging, sorting rows before saving
  async saveFile<DBM extends ObjectWithId>(table: string, _dbms: DBM[]): Promise<void> {
    // Sort the records, if needed
    const dbms = this.sortDBMs(_dbms)

    const op = `saveFile(${table}) ${dbms.length} record(s)`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles([{ type: 'saveBatch', table, dbms }])
    this.logFinished(started, op)
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {
    const op = `saveFiles ${ops.length} op(s):\n${ops
      .map(o => `${o.table} (${o.dbms.length})`)
      .join('\n')}`
    const started = this.logStarted(op)
    await this.cfg.plugin.saveFiles(ops)
    this.logFinished(started, op)
  }

  /**
   * Mutates
   */
  sortDBMs<DBM>(dbms: DBM[]): DBM[] {
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
