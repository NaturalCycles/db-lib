import { EMPTY, Observable } from 'rxjs'
import { BaseDBEntity, CommonDB, CommonDBOptions, CommonDBSaveOptions } from './db.model'
import { DBQuery } from './dbQuery'

export class NoOpDB<DBM extends BaseDBEntity> implements CommonDB<DBM> {
  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]> {
    return []
  }

  async deleteByQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<string[]> {
    return []
  }

  async getByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<DBM[]> {
    return []
  }

  async resetCache (): Promise<void> {}

  async runQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]> {
    return []
  }

  async runQueryCount (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number> {
    return 0
  }

  async saveBatch (table: string, dbms: DBM[], opts?: CommonDBSaveOptions): Promise<DBM[]> {
    return dbms
  }

  streamQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    return EMPTY
  }
}
