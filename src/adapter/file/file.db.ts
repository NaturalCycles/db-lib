import { _by, _sortBy } from '@naturalcycles/js-lib'
import { readableCreate, ReadableTyped } from '@naturalcycles/nodejs-lib'
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
  constructor(public cfg: FileDBCfg) {}

  async ping(): Promise<void> {
    await this.cfg.plugin.ping()
  }

  async getTables(): Promise<string[]> {
    return await this.cfg.plugin.getTables()
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const byId = _by(await this.cfg.plugin.loadFile<DBM>(table), r => r.id)
    return ids.map(id => byId[id]).filter(Boolean)
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {
    // 1. Load the whole file from gh
    const byId = _by(await this.cfg.plugin.loadFile<DBM>(table), r => r.id)

    // 2. Merge with new data (using ids)
    dbms.forEach(dbm => (byId[dbm.id] = dbm))

    // 3. Sort the result, if needed
    const rows = Object.values(byId)
    this.sortDBMs(rows)

    // 4. Save the whole file
    await this.cfg.plugin.saveFile(table, dbms)
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    return {
      records: queryInMemory(q, await this.cfg.plugin.loadFile<DBM>(q.table)),
    }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return (await this.cfg.plugin.loadFile(q.table)).length
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
    let deleted = 0
    const dbms = (await this.cfg.plugin.loadFile<DBM>(table)).filter(dbm => {
      if (ids.includes(dbm.id)) {
        deleted++
        return false
      }
      return true
    })

    if (deleted > 0) {
      this.sortDBMs(dbms)
      await this.cfg.plugin.saveFile(table, dbms)
    }

    return deleted
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const byId = _by(await this.cfg.plugin.loadFile<DBM>(q.table), r => r.id)

    let deleted = 0
    queryInMemory(q, Object.values(byId)).forEach(dbm => {
      delete byId[dbm.id]
      deleted++
    })

    const dbms = Object.values(byId)
    this.sortDBMs(dbms)

    await this.cfg.plugin.saveFile(q.table, dbms)

    return deleted
  }

  // no-op
  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  // no-op
  async resetCache(table?: string): Promise<void> {}

  async getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>> {
    const dbms = await this.cfg.plugin.loadFile(table)
    return CommonSchemaGenerator.generateFromRows({ table }, dbms)
  }

  /**
   * Mutates
   */
  private sortDBMs<DBM>(dbms: DBM[]): void {
    if (this.cfg.sortOnSave) {
      _sortBy(dbms, this.cfg.sortOnSave.name, true)
      if (this.cfg.sortOnSave.descending) dbms.reverse() // mutates
    }
  }
}
