import {
  BaseDBEntity,
  CommonLogger,
  ErrorMode,
  Promisable,
  ZodError,
  ZodSchema,
} from '@naturalcycles/js-lib'
import {
  AjvSchema,
  AjvValidationError,
  JoiValidationError,
  ObjectSchema,
  TransformLogProgressOptions,
  TransformMapOptions,
} from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import { CommonDBCreateOptions, CommonDBOptions, CommonDBSaveOptions } from '../db.model'

export interface CommonDaoHooks<BM extends BaseDBEntity, DBM extends BaseDBEntity> {
  /**
   * Allows to override the id generation function.
   * By default it uses `stringId` from nodejs-lib
   * (which uses lowercase alphanumberic alphabet and the size of 16).
   */
  createRandomId: () => string

  /**
   * createNaturalId hook is called (tried) first.
   * If it doesn't exist - createRandomId is called.
   */
  createNaturalId: (obj: DBM | BM) => string

  /**
   * It's a counter-part of `createNaturalId`.
   * Allows to provide a parser function to parse "natural id" into
   * DBM components (e.g accountId and some other property that is part of the id).
   */
  parseNaturalId: (id: string) => Partial<DBM>

  /**
   * It is called only on `dao.create` method.
   * Dao.create method is called in:
   *
   * - getByIdOrEmpty, getByIdAsDBMOrEmpty
   * - patch, patchAsDBM
   */
  beforeCreate: (bm: Partial<BM>) => Partial<BM>

  /**
   * Called when loading things "as DBM" and validation is not skipped.
   * When loading things as BM/TM - other hooks get involved instead:
   * - beforeDBMToBM
   * - beforeBMToTM
   *
   * TODO: maybe rename those to `validateAs(model)`
   * as it only validates "final state", not intermediate
   */
  beforeDBMValidate: (dbm: Partial<DBM>) => Partial<DBM>

  beforeDBMToBM: (dbm: DBM) => Partial<BM> | Promise<Partial<BM>>
  beforeBMToDBM: (bm: BM) => Partial<DBM> | Promise<Partial<DBM>>

  /**
   * Allows to access the DBM just after it has been loaded from the DB.
   *
   * Normally does nothing.
   *
   * You can change the DBM as you want here: ok to mutate or not, but you need to return the DBM
   * to pass it further.
   *
   * You can return `null` to make it look "not found".
   *
   * You can do validations as needed here and throw errors, they will be propagated.
   */
  afterLoad?: (dbm: DBM) => Promisable<DBM | null>

  /**
   * Allows to access the DBM just before it's supposed to be saved to the DB.
   *
   * Normally does nothing.
   *
   * You can change the DBM as you want here: ok to mutate or not, but you need to return the DBM
   * to pass it further.
   *
   * You can return `null` to prevent it from being saved, without throwing an error.
   * `.save` method will then return the BM/DBM as it has entered the method (it **won't** return the null value!).
   *
   * You can do validations as needed here and throw errors, they will be propagated.
   */
  beforeSave?: (dbm: DBM) => Promisable<DBM | null>

  /**
   * Called in:
   * - dbmToBM (applied before DBM becomes BM)
   * - anyToDBM
   *
   * Hook only allows to apply anonymization to DBM (not to BM).
   * It still applies to BM "transitively", during dbmToBM
   * (e.g after loaded from the Database).
   */
  anonymize: (dbm: DBM) => DBM

  /**
   * If hook is defined - allows to prevent or modify the error thrown.
   * Return `false` to prevent throwing an error.
   * Return original `err` to pass the error through (will be thrown in CommonDao).
   * Return modified/new `Error` if needed.
   */
  onValidationError: (err: JoiValidationError | AjvValidationError | ZodError) => Error | false
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

export interface CommonDaoCfg<BM extends BaseDBEntity, DBM extends BaseDBEntity = BM> {
  db: CommonDB
  table: string

  /**
   * Joi, AjvSchema or ZodSchema is supported.
   */
  dbmSchema?: ObjectSchema<DBM> | AjvSchema<DBM> | ZodSchema<DBM>
  bmSchema?: ObjectSchema<BM> | AjvSchema<BM> | ZodSchema<BM>

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
  hooks?: Partial<CommonDaoHooks<BM, DBM>>

  /**
   * Defaults to true.
   * Set to false to disable auto-generation of `id`.
   * Useful e.g when your DB is generating ids by itself (e.g mysql auto_increment).
   */
  generateId?: boolean

  /**
   * See the same option in CommonDB.
   * Defaults to false normally.
   */
  assignGeneratedIds?: boolean

  /**
   * Defaults to true
   * Set to false to disable `created` field management.
   */
  useCreatedProperty?: boolean

  /**
   * Defaults to true
   * Set to false to disable `updated` field management.
   */
  useUpdatedProperty?: boolean

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

  /**
   * Defaults to false.
   * If true - run patch operations (patch, patchById) in a Transaction.
   *
   * @experimental
   */
  patchInTransaction?: boolean
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
}

export interface CommonDaoSaveOptions<BM extends BaseDBEntity, DBM extends BaseDBEntity>
  extends CommonDaoSaveBatchOptions<DBM> {
  /**
   * If provided - a check will be made.
   * If the object for saving equals to the object passed to `skipIfEquals` - save operation will be skipped.
   *
   * Equality is checked with _deepJsonEquals (aka "deep equals after JSON.stringify/parse", which removes keys with undefined values).
   *
   * It's supposed to be used to prevent "unnecessary saves", when data is not changed.
   */
  skipIfEquals?: BM
}

/**
 * All properties default to undefined.
 */
export interface CommonDaoSaveBatchOptions<DBM extends BaseDBEntity>
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

export interface CommonDaoStreamDeleteOptions<DBM extends BaseDBEntity>
  extends CommonDaoStreamOptions<DBM> {}

export interface CommonDaoStreamSaveOptions<DBM extends BaseDBEntity>
  extends CommonDaoSaveBatchOptions<DBM>,
    CommonDaoStreamOptions<DBM> {}

export interface CommonDaoStreamForEachOptions<IN>
  extends CommonDaoStreamOptions<IN>,
    TransformMapOptions<IN, any> {}

export interface CommonDaoStreamOptions<IN>
  extends CommonDaoOptions,
    TransformLogProgressOptions<IN> {
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

  /**
   * Applicable to some of stream operations, e.g deleteByQuery.
   * If set - `deleteByQuery` won't execute it "all at once", but in batches.
   *
   * Defaults to undefined, so the operation is executed "all at once".
   */
  batchSize?: number

  /**
   * When batchSize is set - this option controls how many batches to run concurrently.
   * Defaults to 16, "the magic number of JavaScript concurrency".
   */
  batchConcurrency?: number
}

export type CommonDaoCreateOptions = CommonDBCreateOptions
