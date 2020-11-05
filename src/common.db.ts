import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  ObjectWithId,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'
import { CommonSchema } from './schema/common.schema'
import { DBTransaction } from './transaction/dbTransaction'

export interface CommonDB {
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

  getTableSchema<ROW extends ObjectWithId>(table: string): Promise<CommonSchema<ROW>>

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
  getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]>

  // QUERY
  /**
   * Order by 'id' is not supported by all implementations (for example, Datastore doesn't support it).
   */
  runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>>

  runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number>

  streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW>

  // SAVE
  saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
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
  /**
   * Should be implemented as a Transaction (best effort), which means that
   * either ALL or NONE of the operations should be applied.
   */
  commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void>
}
