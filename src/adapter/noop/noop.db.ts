import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { Readable } from 'stream'
import { CommonDB } from '../../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  ObjectWithId,
  RunQueryResult,
} from '../../db.model'
import { DBQuery } from '../../query/dbQuery'
import { CommonSchema } from '../../schema/common.schema'
import { DBTransaction } from '../../transaction/dbTransaction'

export class NoOpDB implements CommonDB {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async getTableSchema<DBM>(table: string): Promise<CommonSchema<DBM>> {
    return { table, fields: [] }
  }

  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  async deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    return 0
  }

  async deleteByQuery(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return 0
  }

  async getByIds<DBM extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    return []
  }

  async resetCache(): Promise<void> {}

  async runQuery<OUT>(q: DBQuery, opt?: CommonDBOptions): Promise<RunQueryResult<OUT>> {
    return { records: [] }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    return 0
  }

  async saveBatch<DBM extends ObjectWithId>(
    table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<OUT>(q: DBQuery, opt?: CommonDBOptions): ReadableTyped<OUT> {
    return Readable.from([])
  }

  transaction(): DBTransaction {
    return new DBTransaction(this)
  }
}
