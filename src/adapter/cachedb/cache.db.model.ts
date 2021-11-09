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
   * Pass noopLogger (or undefined) to skip logging completely.
   *
   * Defaults to `console` in dev.
   * Default to noop in AppEngine.
   */
  logger?: CommonLogger
}

export interface CacheDBOptions<ROW extends ObjectWithId> extends CommonDBSaveOptions<ROW> {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

export interface CacheDBStreamOptions<ROW extends ObjectWithId>
  extends CacheDBOptions<ROW>,
    CommonDBStreamOptions {}
export interface CacheDBCreateOptions<ROW extends ObjectWithId>
  extends CacheDBOptions<ROW>,
    CommonDBCreateOptions {}
