import { JsonSchemaObject, JsonSchemaRootObject, ObjectWithId } from '@naturalcycles/js-lib'
import type { ReadableTyped } from '@naturalcycles/nodejs-lib'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonDBTransactionOptions,
  DBPatch,
  DBTransactionFn,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'

export enum CommonDBType {
  'document' = 'document',
  'relational' = 'relational',
}

export interface CommonDB {
  /**
   * Relational databases are expected to return `null` for all missing properties.
   */
  dbType: CommonDBType

  /**
   * Manifest of supported features.
   */
  support: CommonDBSupport

  // Support flags indicate which of the CommonDB features are supported by this implementation.
  supportsQueries?: boolean
  supportsDBQueryFilter?: boolean
  supportsDBQueryFilterIn?: boolean
  supportsDBQueryOrder?: boolean
  supportsDBQuerySelectFields?: boolean
  supportsInsertSaveMethod?: boolean
  supportsUpdateSaveMethod?: boolean
  supportsUpdateByQuery?: boolean
  supportsDBIncrement?: boolean
  supportsCreateTable?: boolean
  supportsTableSchemas?: boolean
  supportsStreaming?: boolean
  supportsBufferValues?: boolean
  supportsNullValues?: boolean
  supportsTransactions?: boolean

  /**
   * Checks that connection/credentials/etc is ok.
   * Also acts as a "warmup request" for a DB.
   * It SHOULD fail if DB setup is wrong (e.g on wrong credentials).
   * It SHOULD succeed if e.g getByIds(['nonExistingKey']) doesn't throw.
   */
  ping: () => Promise<void>

  /**
   * Return all tables (table names) available in this DB.
   */
  getTables: () => Promise<string[]>

  /**
   * $id of the schema SHOULD be like this:
   * `${tableName}.schema.json`
   *
   * This is important for the code to rely on it, and it's verified by dbTest
   */
  getTableSchema: <ROW extends ObjectWithId>(table: string) => Promise<JsonSchemaRootObject<ROW>>

  /**
   * Will do like `create table ...` for mysql.
   * Caution! dropIfExists defaults to false. If set to true - will actually DROP the table!
   */
  createTable: <ROW extends ObjectWithId>(
    table: string,
    schema: JsonSchemaObject<ROW>,
    opt?: CommonDBCreateOptions,
  ) => Promise<void>

  // GET
  /**
   * Order of items returned is not guaranteed to match order of ids.
   * (Such limitation exists because Datastore doesn't support it).
   */
  getByIds: <ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ) => Promise<ROW[]>

  // QUERY
  /**
   * Order by 'id' is not supported by all implementations (for example, Datastore doesn't support it).
   */
  runQuery: <ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ) => Promise<RunQueryResult<ROW>>

  runQueryCount: <ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ) => Promise<number>

  streamQuery: <ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBStreamOptions,
  ) => ReadableTyped<ROW>

  // SAVE
  /**
   * rows can have missing ids only if DB supports auto-generating them (like mysql auto_increment).
   */
  saveBatch: <ROW extends Partial<ObjectWithId>>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions<ROW>,
  ) => Promise<void>

  // DELETE
  /**
   * Returns number of deleted items.
   * Not supported by all implementations (e.g Datastore will always return same number as number of ids).
   */
  deleteByIds: (table: string, ids: string[], opt?: CommonDBOptions) => Promise<number>

  /**
   * Returns number of deleted items.
   * Not supported by all implementations (e.g Datastore will always return same number as number of ids).
   */
  deleteByQuery: <ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ) => Promise<number>

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
  updateByQuery: <ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    patch: DBPatch<ROW>,
    opt?: CommonDBOptions,
  ) => Promise<number>

  // TRANSACTION
  /**
   * Should be implemented as a Transaction (best effort), which means that
   * either ALL or NONE of the operations should be applied.
   *
   * Transaction is automatically committed if fn resolves normally.
   * Transaction is rolled back if fn throws, the error is re-thrown in that case.
   * Graceful rollback is allowed on tx.rollback()
   *
   * By default, transaction is read-write,
   * unless specified as readOnly in CommonDBTransactionOptions.
   */
  runInTransaction: (fn: DBTransactionFn, opt?: CommonDBTransactionOptions) => Promise<void>
}

/**
 * Manifest of supported features.
 */
export interface CommonDBSupport {
  queries?: boolean
  dbQueryFilter?: boolean
  dbQueryFilterIn?: boolean
  dbQueryOrder?: boolean
  dbQuerySelectFields?: boolean
  insertSaveMethod?: boolean
  updateSaveMethod?: boolean
  updateByQuery?: boolean
  dbIncrement?: boolean
  createTable?: boolean
  tableSchemas?: boolean
  streaming?: boolean
  bufferValues?: boolean
  nullValues?: boolean
  transactions?: boolean
}

export const commonDBFullSupport: CommonDBSupport = {
  queries: true,
  dbQueryFilter: true,
  dbQueryFilterIn: true,
  dbQueryOrder: true,
  dbQuerySelectFields: true,
  insertSaveMethod: true,
  updateSaveMethod: true,
  updateByQuery: true,
  dbIncrement: true,
  createTable: true,
  tableSchemas: true,
  streaming: true,
  bufferValues: true,
  nullValues: true,
  transactions: true,
}
