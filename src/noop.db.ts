import { readableFrom } from '@naturalcycles/nodejs-lib'
import { CommonDB } from './common.db'
import { CommonDBOptions, CommonDBSaveOptions, RunQueryResult, SavedDBEntity } from './db.model'
import { DBQuery } from './dbQuery'

export class NoOpDB implements CommonDB {
  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    return 0
  }

  async deleteByQuery(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    return 0
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    return []
  }

  async resetCache(): Promise<void> {}

  async runQuery<OUT>(q: DBQuery, opts?: CommonDBOptions): Promise<RunQueryResult<OUT>> {
    return { records: [] }
  }

  async runQueryCount(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    return 0
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<OUT>(q: DBQuery, opts?: CommonDBOptions): NodeJS.ReadableStream {
    return readableFrom([])
  }
}
