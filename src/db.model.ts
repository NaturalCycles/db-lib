import {
  objectSchema,
  StreamToObservableOptions,
  stringSchema,
  unixTimestampSchema,
  verSchema,
} from '@naturalcycles/nodejs-lib'
import { Merge } from 'type-fest'

/**
 * All properties default to undefined.
 */
export interface CommonDaoOptions extends CommonDBOptions {
  skipValidation?: boolean
  throwOnError?: boolean
  preserveUpdatedCreated?: boolean

  /**
   * If true - data will be anonymized (by calling a BaseDao.anonymize() hook that you can extend in your Dao implementation).
   * Only applicable to loading/querying/streaming_loading operations (n/a for saving).
   * There is additional validation applied AFTER Anonymization, so your anonymization implementation should keep the object valid.
   */
  anonymize?: boolean
}

/**
 * All properties default to undefined.
 */
export interface CommonDaoSaveOptions extends CommonDaoOptions, CommonDBSaveOptions {}

export interface CommonDaoStreamOptions<IN, OUT>
  extends CommonDaoOptions,
    StreamToObservableOptions<IN, OUT> {}

/**
 * All properties default to undefined.
 */
export interface CommonDBOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions extends CommonDBOptions {
  excludeFromIndexes?: string[]
}

export interface RunQueryResult<T> {
  records: T[]
  endCursor?: string
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

export interface CreatedUpdatedVer {
  created: number
  updated: number
  _ver?: number
}

export interface ObjectWithId {
  id: string
}

export interface BaseDBEntity {
  id?: string
  created?: number
  updated?: number
  _ver?: number
}

export interface SavedDBEntity {
  id: string
  created: number
  updated: number
  _ver?: number
}

export type Saved<E> = Merge<E, SavedDBEntity>
export type Unsaved<E> = Merge<E, BaseDBEntity>

export const baseDBEntitySchema = objectSchema<BaseDBEntity>({
  id: stringSchema.optional(),
  created: unixTimestampSchema.optional(),
  updated: unixTimestampSchema.optional(),
  _ver: verSchema.optional(),
})

export const savedDBEntitySchema = objectSchema<SavedDBEntity>({
  id: stringSchema,
  created: unixTimestampSchema,
  updated: unixTimestampSchema,
  _ver: verSchema.optional(),
})
