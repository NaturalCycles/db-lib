import { EMPTY, Observable } from 'rxjs'
import { BaseDBEntity, CommonDB, CommonDBOptions, CommonDBSaveOptions } from './db.model'
import { DBQuery } from './dbQuery'

export class NoOpDB implements CommonDB {
  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]> {
    return []
  }

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<string[]> {
    return []
  }

  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    return []
  }

  async resetCache (): Promise<void> {}

  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    return []
  }

  async runQueryCount<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    return 0
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    return EMPTY
  }
}