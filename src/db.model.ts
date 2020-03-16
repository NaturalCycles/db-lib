import { ErrorMode } from '@naturalcycles/js-lib'
import {
  objectSchema,
  stringSchema,
  TransformLogProgressOptions,
  TransformMapOptions,
  unixTimestampSchema,
  verSchema,
} from '@naturalcycles/nodejs-lib'
import { Merge } from 'type-fest'
import { CommonDB } from './common.db'

/**
 * All properties default to undefined.
 */
export interface CommonDaoOptions extends CommonDBOptions {
  /**
   * If true - will ignore the validation result, but will STILL DO the validation step, which will DO conversion
   * (according to Joi schema).
   *
   * Set skipConversion=true (or raw=true) to bypass conversion step as well (e.g for performance reasons).
   *
   * @default false
   */
  skipValidation?: boolean

  /**
   * If true - will SKIP the joi validation AND conversion steps alltogether. To improve performance of DAO.
   *
   * @default false
   */
  skipConversion?: boolean

  /**
   * If true - will SKIP ANY transformation/processing, will return DB objects as they are. Will also skip created/updated/id
   * generation.
   *
   * Useful for performance/streaming/pipelines.
   *
   * @default false
   */
  raw?: boolean

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

export interface CommonDaoStreamOptions
  extends CommonDaoOptions,
    TransformMapOptions,
    TransformLogProgressOptions {
  /**
   * @default true (for streams)
   */
  skipValidation?: boolean

  /**
   * @default ErrorMode.SUPPRESS for returning ReadableStream, because .pipe() has no concept of "error propagation"
   * @default ErrorMode.SUPPRESS for .forEach() streams as well, but overridable
   */
  errorMode?: ErrorMode
}

export interface CommonDaoCreateOptions extends CommonDBCreateOptions {}

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

export interface CommonDBStreamOptions extends CommonDBOptions {}

export interface CommonDBCreateOptions extends CommonDBOptions {
  /**
   * @default false
   * Caution! If set to true - will actually DROP the table!
   */
  dropIfExists?: boolean
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
