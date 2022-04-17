import { CommonLogger, ErrorMode, ObjectWithId, Saved } from '@naturalcycles/js-lib'
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

export interface CommonDaoHooks<
  BM extends Partial<ObjectWithId<ID>>,
  DBM extends ObjectWithId<ID>,
  TM,
  ID extends string | number,
> {
  createId: (obj: DBM | BM) => ID
  parseNaturalId: (id: ID) => Partial<DBM>
  beforeCreate: (bm: Partial<BM>) => Partial<BM>
  beforeDBMValidate: (dbm: Partial<DBM>) => Partial<DBM>
  beforeDBMToBM: (dbm: DBM) => Partial<BM> | Promise<Partial<BM>>
  beforeBMToDBM: (bm: BM) => Partial<DBM> | Promise<Partial<DBM>>
  beforeTMToBM: (tm: TM) => Partial<BM>
  beforeBMToTM: (bm: BM) => Partial<TM>
  anonymize: (dbm: DBM) => DBM

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
  /**
   * Log operations (e.g "getById returned 1 row"), but not data
   */
  OPERATIONS = 10,
  /**
   * Log operations and data for single operations (e.g getById), but not batch operations.
   */
  DATA_SINGLE = 20,
  /**
   * Log EVERYTHING - all data passing in and out (max 10 rows). Very verbose!
   */
  DATA_FULL = 30,
}

export interface CommonDaoCfg<
  BM extends Partial<ObjectWithId<ID>>,
  DBM extends ObjectWithId<ID> = Saved<BM>,
  TM = BM,
  ID extends string | number = DBM['id'],
> {
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
   * Defaults to false.
   * Setting it to true will set saveMethod to `insert` for save/saveBatch, which will
   * fail for rows that already exist in the DB (if CommonDB implementation supports it).
   *
   * `delete*` and `patch` will throw.
   *
   * You can still override saveMethod, or set opt.allowMutability to allow deletion.
   */
  immutable?: boolean

  /**
   * Defaults to false.
   * Set to true to limit DB writing (will throw an error in such case).
   */
  readOnly?: boolean

  /**
   * Defaults to `console`
   */
  logger?: CommonLogger

  /**
   * @default OPERATIONS
   */
  logLevel?: CommonDaoLogLevel

  /**
   * @default false
   */
  logStarted?: boolean

  // Hooks are designed with inspiration from got/ky interface
  hooks?: Partial<CommonDaoHooks<BM, DBM, TM, ID>>

  /**
   * Defaults to 'string'
   */
  idType?: 'string' | 'number'

  /**
   * Defaults to true.
   * Set to false to disable auto-generation of `id`.
   * Useful e.g when your DB is generating ids by itself (e.g mysql auto_increment).
   */
  createId?: boolean

  /**
   * Defaults to true
   * Set to false to disable `created` field management.
   */
  created?: boolean

  /**
   * Defaults to true
   * Set to false to disable `updated` field management.
   */
  updated?: boolean

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
   * @default false (for streams). Setting to true enables deletion of immutable objects
   */
  allowMutability?: boolean

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

  /**
   * If set - wraps the method in `pTimeout` with a timeout of given number of milliseconds.
   * Currently, it is only used to debug an ongoing GCP infra issue.
   *
   * @experimental
   */
  timeout?: number
}

/**
 * All properties default to undefined.
 */
export interface CommonDaoSaveOptions<DBM extends Partial<ObjectWithId>>
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
