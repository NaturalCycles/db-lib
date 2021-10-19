import { ErrorMode, ObjectWithId } from '@naturalcycles/js-lib'
import {
  AjvSchema,
  AjvValidationError,
  JoiValidationError,
  ObjectSchemaTyped,
  TransformLogProgressOptions,
  TransformMapOptions,
} from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import { CommonDBCreateOptions, CommonDBOptions, CommonDBSaveOptions } from '../db.model'

// Hook DBM, BM, TM types should follow this exact order
export type CommonDaoCreateIdHook<BM, DBM> = (obj: DBM | BM) => string
export type CommonDaoParseNaturalIdHook<DBM> = (id: string) => Partial<DBM>
export type CommonDaoBeforeCreateHook<BM> = (bm: Partial<BM>) => BM
export type CommonDaoBeforeDBMValidateHook<DBM> = (dbm: Partial<DBM>) => Partial<DBM>
export type CommonDaoBeforeDBMToBMHook<BM, DBM> = (dbm: DBM) => Partial<BM> | Promise<Partial<BM>>
export type CommonDaoBeforeBMToDBMHook<BM, DBM> = (bm: BM) => Partial<DBM> | Promise<Partial<DBM>>
export type CommonDaoBeforeTMToBMHook<BM, TM> = (tm: TM) => Partial<BM>
export type CommonDaoBeforeBMToTMHook<BM, TM> = (bm: BM) => Partial<TM>
export type CommonDaoAnonymizeHook<DBM> = (dbm: DBM) => DBM

interface CommonDaoHooks<BM, DBM, TM> {
  createId: CommonDaoCreateIdHook<BM, DBM>
  parseNaturalId: CommonDaoParseNaturalIdHook<DBM>
  beforeCreate: CommonDaoBeforeCreateHook<BM>
  beforeDBMValidate: CommonDaoBeforeDBMValidateHook<DBM>
  beforeDBMToBM: CommonDaoBeforeDBMToBMHook<BM, DBM>
  beforeBMToDBM: CommonDaoBeforeBMToDBMHook<BM, DBM>
  beforeTMToBM: CommonDaoBeforeTMToBMHook<BM, TM>
  beforeBMToTM: CommonDaoBeforeBMToTMHook<BM, TM>
  anonymize: CommonDaoAnonymizeHook<DBM>

  /**
   * If hook is defined - allows to prevent or modify the error thrown.
   * Return `false` to prevent throwing an error.
   * Return original `err` to pass the error through (will be thrown in CommonDao).
   * Return modified/new `Error` if needed.
   */
  onValidationError: (err: JoiValidationError | AjvValidationError) => Error | false
}

export enum CommonDaoLogLevel {
  /**
   * Same as undefined
   */
  NONE = 0,
  OPERATIONS = 10,
  DATA_SINGLE = 20,
  DATA_FULL = 30,
}

export interface CommonDaoCfg<BM extends Partial<ObjectWithId>, DBM extends ObjectWithId, TM> {
  db: CommonDB
  table: string

  /**
   * Joi or AjvSchema are supported.
   */
  dbmSchema?: ObjectSchemaTyped<DBM> | AjvSchema<DBM>
  bmSchema?: ObjectSchemaTyped<BM> | AjvSchema<BM>
  tmSchema?: ObjectSchemaTyped<TM> | AjvSchema<TM>

  excludeFromIndexes?: (keyof DBM)[]

  /**
   * @default to false
   * Set to true to limit DB writing (will throw an error is such case).
   */
  readOnly?: boolean

  /**
   * @default OPERATIONS
   */
  logLevel?: CommonDaoLogLevel

  /**
   * @default false
   */
  logStarted?: boolean

  // Hooks are designed with inspiration from got/ky interface
  hooks?: Partial<CommonDaoHooks<BM, DBM, TM>>

  /**
   * @default true
   * Set to false to disable created/updated fields management.
   */
  createdUpdated?: boolean

  /**
   * Default is false.
   * If true - will run `_filterNullishValues` inside `validateAndConvert` function
   * (instead of `_filterUndefinedValues`).
   * This was the old db-lib behavior.
   * This option allows to keep backwards-compatible behavior.
   *
   * @deprecated
   */
  filterNullishValues?: boolean
}

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
export interface CommonDaoSaveOptions<DBM extends ObjectWithId>
  extends CommonDaoOptions,
    CommonDBSaveOptions<DBM> {
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

export interface CommonDaoStreamForEachOptions<IN>
  extends CommonDaoStreamOptions,
    TransformMapOptions<IN, any>,
    TransformLogProgressOptions<IN> {}

export interface CommonDaoStreamOptions extends CommonDaoOptions {
  /**
   * @default true (for streams)
   */
  skipValidation?: boolean

  /**
   * @default true (for streams)
   */
  skipConversion?: boolean

  /**
   * @default ErrorMode.SUPPRESS for returning ReadableStream, because .pipe() has no concept of "error propagation"
   * @default ErrorMode.SUPPRESS for .forEach() streams as well, but overridable
   */
  errorMode?: ErrorMode
}

export type CommonDaoCreateOptions = CommonDBCreateOptions
