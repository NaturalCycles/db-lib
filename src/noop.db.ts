import { EMPTY, Observable } from 'rxjs'
import { CommonDB } from './common.db'
import { BaseDBEntity, CommonDBOptions, CommonDBSaveOptions, RunQueryResult } from './db.model'
import { DBQuery } from './dbQuery'

export class NoOpDB implements CommonDB {
  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    return 0
  }

  async deleteByQuery<DBM extends BaseDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    return 0
  }

  async getByIds<DBM extends BaseDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    return []
  }

  async resetCache(): Promise<void> {}

  async runQuery<DBM extends BaseDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<RunQueryResult<DBM>> {
    return { records: [] }
  }

  async runQueryCount<DBM extends BaseDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    return 0
  }

  async saveBatch<DBM extends BaseDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<DBM extends BaseDBEntity>(q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    return EMPTY
  }
}
