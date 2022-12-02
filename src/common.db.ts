import { JsonSchemaObject, JsonSchemaRootObject, ObjectWithId } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  DBPatch,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'
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

  /**
   * $id of the schema SHOULD be like this:
   * `${tableName}.schema.json`
   *
   * This is important for the code to rely on it, and it's verified by dbTest
   */
  getTableSchema<ROW extends ObjectWithId>(table: string): Promise<JsonSchemaRootObject<ROW>>

  /**
   * Will do like `create table ...` for mysql.
   * Caution! dropIfExists defaults to false. If set to true - will actually DROP the table!
   */
  createTable<ROW extends ObjectWithId>(
    table: string,
    schema: JsonSchemaObject<ROW>,
    opt?: CommonDBCreateOptions,
  ): Promise<void>

  // QUERY
  /**
   * Order by 'id' is not supported by all implementations (for example, Datastore doesn't support it).
   */
  runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>>

  runQueryCount<ROW extends ObjectWithId>(q: DBQuery<ROW>, opt?: CommonDBOptions): Promise<number>

  streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ): ReadableTyped<ROW>

  // SAVE
  /**
   * rows can have missing ids only if DB supports auto-generating them (like mysql auto_increment).
   */
  saveBatch<ROW extends Partial<ObjectWithId>>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions<ROW>,
  ): Promise<void>

  // DELETE
  /**
   * Returns number of deleted items.
   * Not supported by all implementations (e.g Datastore will always return same number as number of ids).
   */
  deleteByQuery<ROW extends ObjectWithId>(q: DBQuery<ROW>, opt?: CommonDBOptions): Promise<number>

  /**
   * Applies patch to the rows returned by the query.
   *
   * Example:
   *
   * UPDATE table SET A = B where $QUERY_CONDITION
   *
   * patch would be { A: 'B' } for that query.
   *
   * Supports "increment query", example:
   *
   * UPDATE table SET A = A + 1
   *
   * In that case patch would look like:
   * { A: DBIncrement(1) }
   *
   * Returns number of rows affected.
   */
  updateByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    patch: DBPatch<ROW>,
    opt?: CommonDBOptions,
  ): Promise<number>

  // TRANSACTION
  /**
   * Should be implemented as a Transaction (best effort), which means that
   * either ALL or NONE of the operations should be applied.
   */
  commitTransaction(tx: DBTransaction, opt?: CommonDBOptions): Promise<void>
}
