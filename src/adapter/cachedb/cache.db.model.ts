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
}

export interface CacheDBOptions extends CommonDBSaveOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

export interface CacheDBStreamOptions extends CacheDBOptions, CommonDBStreamOptions {}
export interface CacheDBCreateOptions extends CacheDBOptions, CommonDBCreateOptions {}
