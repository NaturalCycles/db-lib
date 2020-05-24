import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  RunQueryResult,
  SavedDBEntity,
} from './db.model'
import { DBQuery } from './dbQuery'
import { DBTransaction } from './dbTransaction'
import { CommonSchema } from './schema/common.schema'

export interface CommonDB {
  /**
   * If table not specified - reset all DB tables.
   */
  resetCache(table?: string): Promise<void>

  /**
   * Checks that connection/credentials/etc is ok.
   * Also acts as a "warmup request" for a DB.
   * It SHOULD fail if DB setup is wrong (e.g on wrong credentials).
   * It SHOULD succeed if e.g getByIds(['nonExistingKey']) doesn't throw.
   */
  ping(): Promise<void>

  /**
   * Return all tables (table names) available in this DB.
   */
  getTables(): Promise<string[]>

  getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>>

  /**
   * Will do like `create table ...` for mysql.
   * Caution! dropIfExists defaults to false. If set to true - will actually DROP the table!
   */
  createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void>

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
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<OUT>

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

  // TRANSACTION
  transaction(): DBTransaction

  // commitTransaction(tx: DBTransaction): Promise<void>
}
