import { ErrorMode } from '@naturalcycles/js-lib'
import { TransformLogProgressOptions, TransformMapOptions } from '@naturalcycles/nodejs-lib'
import { CommonDBCreateOptions, CommonDBOptions, CommonDBSaveOptions } from '../db.model'

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

  /**
   * @default inherited from CommonDaoCfg.throwOnEntityValidationError
   */
  throwOnError?: boolean

  /**
   * @default false
   */
  preserveUpdatedCreated?: boolean

  /**
   * If true - data will be anonymized (by calling a BaseDao.anonymize() hook that you can extend in your Dao implementation).
   * Only applicable to loading/querying/streaming_loading operations (n/a for saving).
   * There is additional validation applied AFTER Anonymization, so your anonymization implementation should keep the object valid.
   */
  anonymize?: boolean

  /**
   * Allows to override the Table that this Dao is connected to, only in the context of this call.
   *
   * Useful e.g in AirtableDB where you can have one Dao to control multiple tables.
   */
  table?: string
}

/**
 * All properties default to undefined.
 */
export interface CommonDaoSaveOptions extends CommonDaoOptions, CommonDBSaveOptions {
  /**
   * @default false
   *
   * True would make sure that auto-generated id (only auto-generated, not passed!) is unique (not already present in DB).
   * If id is already present - auto-generator will retry auto-generating it few times, until it finds unused id.
   * If failed X times - will throw an error.
   *
   * Only applies to auto-generated ids! Does not apply to passed id.
   */
  ensureUniqueId?: boolean
}

export interface CommonDaoStreamForEachOptions
  extends CommonDaoStreamOptions,
    TransformLogProgressOptions {}

export interface CommonDaoStreamOptions extends CommonDaoOptions, TransformMapOptions {
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
