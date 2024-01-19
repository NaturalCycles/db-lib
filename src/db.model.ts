import type { PartialObjectWithId } from '@naturalcycles/js-lib'
import { CommonDB } from './common.db'

/**
 * Similar to SQL INSERT, UPDATE.
 * Insert will fail if row already exists.
 * Update will fail if row is missing.
 * Upsert will auto-detect and use Insert or Update to not fail.
 *
 * Default is Upsert.
 */
export type CommonDBSaveMethod = 'upsert' | 'insert' | 'update'

/**
 * Transaction is committed when the function returns resolved Promise (aka "returns normally").
 *
 * Transaction is rolled back when the function returns rejected Promise (aka "throws").
 */
export type DBTransactionFn = (tx: DBTransaction) => Promise<void>

/**
 * Transaction context.
 * Has similar API than CommonDB, but all operations are performed in the context of the transaction.
 */
export interface DBTransaction {
  getByIds: CommonDB['getByIds']
  saveBatch: CommonDB['saveBatch']
  deleteByIds: CommonDB['deleteByIds']

  /**
   * Perform a graceful rollback.
   * It'll rollback the transaction and won't throw/re-throw any errors.
   */
  rollback: () => Promise<void>
}

export interface CommonDBTransactionOptions {
  /**
   * Default is false.
   * If set to true - Transaction is created as read-only.
   */
  readOnly?: boolean
}

export interface CommonDBOptions {
  /**
   * If passed - the operation will be performed in the context of that DBTransaction.
   * Note that not every type of operation supports Transaction
   * (e.g in Datastore queries cannot be executed inside a Transaction).
   * Also, not every CommonDB implementation supports Transactions.
   */
  tx?: DBTransaction
}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions<ROW extends PartialObjectWithId = any>
  extends CommonDBOptions {
  excludeFromIndexes?: (keyof ROW)[]

  /**
   * Default is `upsert`
   */
  saveMethod?: CommonDBSaveMethod

  /**
   * Only applicable to tables where id is "auto-generated by DB", e.g `auto_increment` in MySQL.
   * By default it's false, so, auto-generated id will NOT be assigned/returned.
   * Setting it to true will assign and return auto-generated id (on all rows, one by one).
   * It's not true by default, because getting auto-generated id incurs an overhead of doing extra call (e.g LAST_INSERT_ID() in MySQL).
   */
  assignGeneratedIds?: boolean
}

export type CommonDBStreamOptions = CommonDBOptions

export interface CommonDBCreateOptions extends CommonDBOptions {
  /**
   * Caution! If set to true - will actually DROP the table!
   *
   * @default false
   */
  dropIfExists?: boolean
}

export interface RunQueryResult<T> {
  rows: T[]
  endCursor?: string
}

export type DBOperation = DBSaveBatchOperation | DBDeleteByIdsOperation

export interface DBSaveBatchOperation<ROW extends PartialObjectWithId = any> {
  type: 'saveBatch'
  table: string
  rows: ROW[]
  opt?: CommonDBSaveOptions<ROW>
}

export interface DBDeleteByIdsOperation {
  type: 'deleteByIds'
  table: string
  ids: string[]
  opt?: CommonDBOptions
}

export enum DBRelation {
  ONE_TO_ONE = 'ONE_TO_ONE',
  ONE_TO_MANY = 'ONE_TO_MANY',
}

export enum DBModelType {
  DBM = 'DBM',
  BM = 'BM',
  TM = 'TM',
}

/**
 * Allows to construct a query similar to:
 *
 * UPDATE table SET A = A + 1
 *
 * In this case DBIncement.of(1) will be needed.
 */
export class DBIncrement {
  private constructor(public amount: number) {}

  static of(amount: number): DBIncrement {
    return new DBIncrement(amount)
  }
}

export type DBPatch<ROW extends PartialObjectWithId> = Partial<
  Record<keyof ROW, ROW[keyof ROW] | DBIncrement>
>
