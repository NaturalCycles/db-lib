import { Observable } from 'rxjs'
import { BaseDBEntity, CommonDBOptions, CommonDBSaveOptions } from './db.model'
import { DBQuery } from './dbQuery'

export interface CommonDB {
  /**
   * If table not specified - reset all DB.
   */
  resetCache (table?: string): Promise<void>

  // GET
  /**
   * Order of items returned is not guaranteed to match order of ids.
   * (Such limitation exists because Datastore doesn't support it).
   */
  getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]>

  // QUERY
  /**
   * Order by 'id' is not supported by all implementations (for example, Datastore doesn't support it).
   */
  runQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]>
  runQueryCount<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number>
  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM>

  // SAVE
  saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void>

  // DELETE
  /**
   * @returns number of deleted items.
   * Not supported by all implementations (e.g Datastore will always return same number as number of ids).
   */
  deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<number>
  deleteByQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number>
}
