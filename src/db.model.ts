import { AnyObjectWithId, ObjectWithId } from '@naturalcycles/js-lib'

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
