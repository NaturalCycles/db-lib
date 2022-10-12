import { ObjectWithId } from '@naturalcycles/js-lib'

/**
 * Similar to SQL INSERT, UPDATE.
 * Insert will fail if row already exists.
 * Update will fail if row is missing.
 * Upsert will auto-detect and use Insert or Update to not fail.
 *
 * Default is Upsert.
 */
export type CommonDBSaveMethod = 'upsert' | 'insert' | 'update'

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface CommonDBOptions {}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions<ROW extends Partial<ObjectWithId> = any>
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

export interface DBSaveBatchOperation<ROW extends Partial<ObjectWithId> = any> {
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
