import { AnyObject } from '@naturalcycles/js-lib'
import { CommonDB } from './common.db'

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface CommonDBOptions {}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions<ROW extends ObjectWithId = AnyObjectWithId>
  extends CommonDBOptions {
  excludeFromIndexes?: (keyof ROW)[]
}

export type CommonDBStreamOptions = CommonDBOptions

export interface CommonDBCreateOptions extends CommonDBOptions {
  /**
   * @default false
   * Caution! If set to true - will actually DROP the table!
   */
  dropIfExists?: boolean
}

export interface RunQueryResult<T> {
  rows: T[]
  endCursor?: string
}

export type DBOperation = DBSaveBatchOperation | DBDeleteByIdsOperation

export interface DBSaveBatchOperation<ROW extends ObjectWithId = AnyObjectWithId> {
  type: 'saveBatch'
  table: string
  rows: ROW[]
}

export interface DBDeleteByIdsOperation {
  type: 'deleteByIds'
  table: string
  ids: string[]
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

export interface CreatedUpdated {
  created: number
  updated: number
}

export interface CreatedUpdatedId extends CreatedUpdated {
  id: string
}

export interface ObjectWithId {
  id: string
}

export interface AnyObjectWithId extends AnyObject, ObjectWithId {}

/**
 * Interface for a module (lib) that implements CommonDB.
 *
 * Example:
 *
 * const lib: CommonDBModule = require('mysql-lib')
 * const db = lib.getDB()
 */
export interface CommonDBAdapter {
  /**
   * @param cfg was read from SECRET_DB${i} by secret('SECRET_DB${i}') method and passed there.
   * It's a string that can contain e.g JSON.stringified configuration object (depends on the adapter).
   */
  getDBAdapter(cfg?: string): CommonDB
}
