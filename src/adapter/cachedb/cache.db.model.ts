import { CommonLogger, ObjectWithId } from '@naturalcycles/js-lib'
import { CommonDB } from '../../common.db'
import { CommonDBCreateOptions, CommonDBSaveOptions, CommonDBStreamOptions } from '../../db.model'

export interface CacheDBCfg {
  name: string
  cacheDB: CommonDB
  downstreamDB: CommonDB

  /**
   * If true - cache SAVING operations will await to be completed,
   * otherwise will be executed async
   *
   * @default false
   */
  awaitCache?: boolean

  /**
   * Global default.
   *
   * @default false
   */
  skipCache?: boolean

  /**
   * Global default.
   *
   * @default false
   */
  onlyCache?: boolean

  /**
   * @default false
   */
  logCached?: boolean

  /**
   * @default false
   */
  logDownstream?: boolean

  /**
   * Defaults to `console`.
   */
  logger?: CommonLogger
}

export interface CacheDBOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

export interface CacheDBSaveOptions<ROW extends Partial<ObjectWithId>>
  extends CacheDBOptions,
    CommonDBSaveOptions<ROW> {}

export interface CacheDBStreamOptions extends CacheDBOptions, CommonDBStreamOptions {}

export interface CacheDBCreateOptions extends CacheDBOptions, CommonDBCreateOptions {}
