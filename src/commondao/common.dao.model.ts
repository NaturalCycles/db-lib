import { ErrorMode } from '@naturalcycles/js-lib'
import {
  JoiValidationError,
  ObjectSchemaTyped,
  TransformLogProgressOptions,
} from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  ObjectWithId,
} from '../db.model'

// Hook DBM, BM, TM types should follow this exact order
export type CommonDaoCreateIdHook<BM, DBM> = (obj: DBM | BM) => string
export type CommonDaoParseNaturalIdHook<DBM> = (id: string) => Partial<DBM>
export type CommonDaoBeforeCreateHook<BM> = (bm: Partial<BM>) => BM
export type CommonDaoBeforeDBMValidateHook<DBM> = (dbm: DBM) => DBM
export type CommonDaoBeforeDBMToBMHook<BM, DBM> = (dbm: DBM) => BM
export type CommonDaoBeforeBMToDBMHook<BM, DBM> = (bm: BM) => DBM
export type CommonDaoBeforeTMToBMHook<BM, TM> = (tm: TM) => BM
export type CommonDaoBeforeBMToTMHook<BM, TM> = (bm: BM) => TM
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
  dbmSchema?: ObjectSchemaTyped<DBM>
  bmSchema?: ObjectSchemaTyped<BM>
  tmSchema?: ObjectSchemaTyped<TM>

  excludeFromIndexes?: string[]

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

  /**
   * @default false
   */
  throwOnEntityValidationError?: boolean

  /**
   * @default to throwOnEntityValidationError setting
   */
  throwOnDaoCreateObject?: boolean

  /**
   * Called when validation error occurs.
   * Called ONLY when error is NOT thrown (when throwOnEntityValidationError is off)
   */
  onValidationError?: (err: JoiValidationError) => any

  // Hooks are designed with inspiration from got/ky interface
  hooks?: Partial<CommonDaoHooks<BM, DBM, TM>>

  /**
   * @default true
   * Set to false to disable created/updated fields management.
   */
  createdUpdated?: boolean
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

export interface CommonDaoStreamForEachOptions<IN>
  extends CommonDaoStreamOptions,
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
