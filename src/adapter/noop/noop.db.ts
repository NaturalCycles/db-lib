import { readableFromArray, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  RunQueryResult,
  SavedDBEntity,
} from '../../db.model'
import { DBQuery } from '../../dbQuery'
import { CommonSchema } from '../../schema/common.schema'

export class NoOpDB implements CommonDB {
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

  async getByIds<DBM extends SavedDBEntity>(
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

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<OUT>(q: DBQuery, opt?: CommonDBOptions): ReadableTyped<OUT> {
    return readableFromArray([])
  }
}
