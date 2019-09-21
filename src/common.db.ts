import { Observable } from 'rxjs'
import { CommonDBOptions, CommonDBSaveOptions, RunQueryResult, SavedDBEntity } from './db.model'
import { DBQuery } from './dbQuery'

export interface CommonDB {
  /**
   * If table not specified - reset all DB.
   */
  resetCache(table?: string): Promise<void>

  // GET
  /**
   * Order of items returned is not guaranteed to match order of ids.
   * (Such limitation exists because Datastore doesn't support it).
   */
  getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]>

  // QUERY
  /**
   * Order by 'id' is not supported by all implementations (for example, Datastore doesn't support it).
   *
   * DBM is included in generics, so it infer OUT from DBQuery<DBM>
   */
  runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>>

  runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number>

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Observable<OUT>

  // SAVE
  saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void>

  // DELETE
  /**
   * @returns number of deleted items.
   * Not supported by all implementations (e.g Datastore will always return same number as number of ids).
   */
  deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number>

  deleteByQuery(q: DBQuery, opt?: CommonDBOptions): Promise<number>
}
