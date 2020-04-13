import { AppError, ErrorMode, Mapper, since, _truncate } from '@naturalcycles/js-lib'
import {
  Debug,
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
  ReadableTyped,
  stringId,
  transformLogProgress,
  transformMap,
  transformTap,
  writableForEach,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import { CommonDB } from './common.db'
import {
  BaseDBEntity,
  CommonDaoCreateOptions,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
  DBModelType,
  ObjectWithId,
  RunQueryResult,
  Saved,
  SavedDBEntity,
} from './db.model'
import { DBQuery, RunnableDBQuery } from './dbQuery'
import { DBLibError } from './index'
import { CommonSchema } from './schema/common.schema'

export enum CommonDaoLogLevel {
  /**
   * Same as undefined
   */
  NONE = 0,
  OPERATIONS = 10,
  DATA_SINGLE = 20,
  DATA_FULL = 30,
}

export interface CommonDaoCfg<BM extends BaseDBEntity, DBM extends SavedDBEntity, TM> {
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
  throwOnDaoCreateObject?: boolean

  /**
   * Called when validation error occurs.
   * Called ONLY when error is NOT thrown (when throwOnEntityValidationError is off)
   */
  onValidationError?: (err: JoiValidationError) => any
}

const log = Debug('nc:db-lib:commondao')

/**
 * Lowest common denominator API between supported Databases.
 *
 * DBM = Database model (how it's stored in DB)
 * BM = Backend model (optimized for API access)
 * TM = Transport model (optimized to be sent over the wire)
 */
export class CommonDao<
  BM extends BaseDBEntity = any,
  DBM extends SavedDBEntity = Saved<BM>,
  TM = BM
> {
  constructor(public cfg: CommonDaoCfg<BM, DBM, TM>) {
    this.cfg = {
      logLevel: CommonDaoLogLevel.OPERATIONS,
      ...cfg,
    }
  }

  /**
   * To be extended
   */
  createId(obj: DBM | BM): string {
    return stringId()
  }

  /**
   * To be extended
   */
  parseNaturalId(id: string): Partial<DBM> {
    return {}
  }

  /**
   * To be extended
   */
  beforeCreate(bm: BM): BM {
    return bm
  }

  /**
   * To be extended
   */
  beforeDBMValidate(dbm: DBM): DBM {
    return dbm
  }

  /**
   * To be extended
   */
  async beforeDBMToBM(dbm: DBM): Promise<BM> {
    return dbm as any
  }

  /**
   * To be extended
   */
  async beforeBMToDBM(bm: BM): Promise<DBM> {
    return bm as any
  }

  /**
   * To be extended
   */
  async beforeTMToBM(tm: TM): Promise<BM> {
    return tm as any
  }

  /**
   * To be extended
   */
  async beforeBMToTM(bm: Saved<BM>): Promise<TM> {
    return bm as any
  }

  /**
   * To be extended
   */
  anonymize(dbm: DBM): DBM {
    return dbm
  }

  // CREATE
  create(input: BM, opt: CommonDaoOptions = {}): Saved<BM> {
    if (opt.throwOnError === undefined) {
      opt.throwOnError = this.cfg.throwOnDaoCreateObject
    }

    let bm = { ...this.beforeCreate(input) }
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // If no SCHEMA - return as is
    return this.assignIdCreatedUpdated(bm, opt)
  }

  // GET
  async getById(id?: string, opt: CommonDaoOptions = {}): Promise<Saved<BM> | undefined> {
    if (!id) return
    const op = `getById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    const bm = opt.raw ? (dbm as any) : await this.dbmToBM(dbm, opt)
    this.logResult(started, op, bm, table)
    return bm
  }

  async getByIdOrCreate(id: string, bmToCreate: BM, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ ...bmToCreate, id }, opt)
  }

  async getByIdOrEmpty(id: string, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ id } as BM, opt)
  }

  async getByIdAsDBMOrEmpty(id: string, opt?: CommonDaoOptions): Promise<DBM> {
    const dbm = await this.getByIdAsDBM(id, opt)
    if (dbm) return dbm

    return this.create({ id } as BM, opt) as any
  }

  async getByIdAsDBM(id?: string, opt: CommonDaoOptions = {}): Promise<DBM | undefined> {
    if (!id) return
    const op = `getByIdAsDBM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (!opt.raw) {
      dbm = this.anyToDBM(dbm, opt)
    }
    this.logResult(started, op, dbm, table)
    return dbm
  }

  async getByIdAsTM(id?: string, opt: CommonDaoOptions = {}): Promise<TM | undefined> {
    if (!id) return
    const op = `getByIdAsTM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (opt.raw) {
      this.logResult(started, op, dbm, table)
      return dbm as any
    }
    const bm = await this.dbmToBM(dbm, opt)
    const tm = await this.bmToTM(bm, opt)
    this.logResult(started, op, tm, table)
    return tm
  }

  async getByIds(ids: string[], opt: CommonDaoOptions = {}): Promise<Saved<BM>[]> {
    const op = `getByIds ${ids.length} id(s) (${_truncate(ids.slice(0, 10).join(', '), 50)})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const dbms = await this.cfg.db.getByIds<DBM>(table, ids)
    const bms = opt.raw ? (dbms as any) : await this.dbmsToBM(dbms, opt)
    this.logResult(started, op, bms, table)
    return bms
  }

  async requireById(id: string, opt: CommonDaoOptions = {}): Promise<Saved<BM>> {
    const r = await this.getById(id, opt)
    if (!r) {
      this.throwRequiredError(id, opt)
    }
    return r
  }

  async requireByIdAsDBM(id: string, opt: CommonDaoOptions = {}): Promise<DBM> {
    const r = await this.getByIdAsDBM(id, opt)
    if (!r) {
      this.throwRequiredError(id, opt)
    }
    return r
  }

  private throwRequiredError(id: string, opt: CommonDaoOptions): never {
    const table = opt.table || this.cfg.table
    throw new AppError(`DB record required, but not found: ${table}.${id}`, {
      code: DBLibError.DB_RECORD_REQUIRED,
      table,
      id,
    })
  }

  /**
   * Throws if readOnly is true
   */
  private requireWriteAccess(): void {
    if (this.cfg.readOnly) {
      throw new AppError(DBLibError.DAO_IS_READ_ONLY, {
        code: DBLibError.DAO_IS_READ_ONLY,
        table: this.cfg.table,
      })
    }
  }

  async getBy(by: string, value: any, limit = 0, opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.query().filter(by, '=', value).limit(limit).runQuery(opt)
  }

  async getOneBy(by: string, value: any, opt?: CommonDaoOptions): Promise<Saved<BM> | undefined> {
    const [bm] = await this.query().filter(by, '=', value).limit(1).runQuery(opt)
    return bm
  }

  async getAll(opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.query().runQuery(opt)
  }

  // QUERY
  /**
   * Pass `table` to override table
   */
  query(table?: string): RunnableDBQuery<BM, DBM, TM> {
    return new RunnableDBQuery<BM, DBM, TM>(this, table)
  }

  async runQuery<OUT = Saved<BM>>(q: DBQuery<BM, DBM, TM>, opt?: CommonDaoOptions): Promise<OUT[]> {
    const { records } = await this.runQueryExtended<OUT>(q, opt)
    return records
  }

  async runQueryExtended<OUT = Saved<BM>>(
    q: DBQuery<BM, DBM, TM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<OUT>> {
    const op = `runQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { records, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const bms = partialQuery || opt.raw ? (records as any[]) : await this.dbmsToBM(records, opt)
    this.logResult(started, op, bms, q.table)
    return {
      records: bms,
      ...queryResult,
    }
  }

  async runQueryAsDBM<OUT = DBM>(q: DBQuery<BM, DBM>, opt?: CommonDaoOptions): Promise<OUT[]> {
    const { records } = await this.runQueryExtendedAsDBM<OUT>(q, opt)
    return records
  }

  async runQueryExtendedAsDBM<OUT = DBM>(
    q: DBQuery<BM, DBM, TM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<OUT>> {
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { records, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const dbms = partialQuery || opt.raw ? records : this.anyToDBMs(records, opt)
    this.logResult(started, op, dbms, q.table)
    return { records: (dbms as any) as OUT[], ...queryResult }
  }

  async runQueryCount(q: DBQuery<BM, DBM, TM>, opt: CommonDaoOptions = {}): Promise<number> {
    const op = `runQueryCount(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const count = await this.cfg.db.runQueryCount(q, opt)
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} row(s) in ${since(started)}`)
    }
    return count
  }

  async streamQueryForEach<IN = Saved<BM>, OUT = IN>(
    q: DBQuery<BM, DBM, TM>,
    mapper: Mapper<OUT, void>,
    opt: CommonDaoStreamForEachOptions = {},
  ): Promise<void> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM, OUT>(q, opt),
      transformMap<any, DBM>(dbm => (partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)), opt),
      transformMap<DBM, Saved<BM>>(
        async dbm => (partialQuery || opt.raw ? (dbm as any) : await this.dbmToBM(dbm, opt)),
        opt,
      ),
      transformTap(() => count++),
      transformLogProgress({
        metric: q.table,
        ...opt,
      }),
      writableForEach<OUT>(mapper, opt),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} row(s) in ${since(started)}`)
    }
  }

  async streamQueryAsDBMForEach<IN = DBM, OUT = IN>(
    q: DBQuery<BM, DBM, TM>,
    mapper: Mapper<OUT, void>,
    opt: CommonDaoStreamForEachOptions = {},
  ): Promise<void> {
    if (opt.skipValidation === undefined) opt.skipValidation = true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryAsDBMForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM, OUT>(q, opt),
      transformMap<any, OUT>(dbm => (partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)), opt),
      transformTap(() => count++),
      transformLogProgress<OUT>({
        metric: q.table,
        ...opt,
      }),
      writableForEach<OUT>(mapper, opt),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} row(s) in ${since(started)}`)
    }
  }

  /**
   * Stream as Readable, to be able to .pipe() it further with support of backpressure.
   */
  streamQueryAsDBM<OUT = DBM>(
    q: DBQuery<BM, DBM, TM>,
    opt: CommonDaoStreamOptions = {},
  ): ReadableTyped<OUT> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames

    const stream = this.cfg.db.streamQuery<DBM, OUT>(q, opt)
    if (partialQuery || opt.raw) return stream

    return stream.pipe(
      transformMap<any, DBM>(dbm => this.anyToDBM(dbm, opt), {
        ...opt,
        errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
      }),
    )
  }

  /**
   * Stream as Readable, to be able to .pipe() it further with support of backpressure.
   */
  streamQuery<OUT = Saved<BM>>(
    q: DBQuery<BM, DBM, TM>,
    opt: CommonDaoStreamOptions = {},
  ): ReadableTyped<OUT> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const stream = this.cfg.db.streamQuery<DBM, OUT>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    if (partialQuery || opt.raw) return stream

    const safeOpt = {
      ...opt,
      errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
    }

    return stream
      .pipe(transformMap<any, DBM>(dbm => this.anyToDBM(dbm, opt), safeOpt))
      .pipe(transformMap<DBM, Saved<BM>>(async dbm => await this.dbmToBM(dbm, opt), safeOpt))
  }

  async queryIds(q: DBQuery<BM, DBM>, opt?: CommonDaoOptions): Promise<string[]> {
    const { records } = await this.cfg.db.runQuery<DBM, ObjectWithId>(q.select(['id']), opt)
    return records.map(r => r.id)
  }

  streamQueryIds(q: DBQuery<BM, DBM>, opt: CommonDaoStreamOptions = {}): ReadableTyped<string> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    return this.cfg.db.streamQuery<DBM>(q.select(['id']), opt).pipe(
      transformMap<ObjectWithId, string>(objectWithId => objectWithId.id, {
        ...opt,
        errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
      }),
    )
  }

  async streamQueryIdsForEach(
    q: DBQuery<BM, DBM>,
    mapper: Mapper<string, void>,
    opt: CommonDaoStreamForEachOptions = {},
  ): Promise<void> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const op = `streamQueryIdsForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM>(q.select(['id']), opt),
      transformMap<ObjectWithId, string>(objectWithId => objectWithId.id, opt),
      transformTap(() => count++),
      transformLogProgress<string>({
        metric: q.table,
        ...opt,
      }),
      writableForEach<string>(mapper, opt),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} id(s) in ${since(started)}`)
    }
  }

  assignIdCreatedUpdated<T extends DBM | BM>(obj: T, opt: CommonDaoOptions = {}): Saved<T> {
    const now = Math.floor(Date.now() / 1000)

    return {
      ...obj,
      id: obj.id || this.createId(obj),
      created: obj.created || obj.updated || now,
      updated: opt.preserveUpdatedCreated && obj.updated ? obj.updated : now,
    }
  }

  // SAVE
  async save(bm: BM, opt: CommonDaoSaveOptions = {}): Promise<Saved<BM>> {
    this.requireWriteAccess()
    const dbm = await this.bmToDBM(bm, opt) // does assignIdCreatedUpdated
    const op = `save(${dbm.id})`
    const table = opt.table || this.cfg.table
    const started = this.logSaveStarted(op, bm, table)
    await this.cfg.db.saveBatch(table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    const savedBM = await this.dbmToBM(dbm, opt)
    this.logSaveResult(started, op, table)
    return savedBM
  }

  /**
   * Loads the record by id.
   * Creates the record (via this.create()) if it doesn't exist
   * (this will cause a validation error if Patch has not enough data for the record to be valid).
   * Saves (as fast as possible) with the Patch applied.
   *
   * Convenience method to replace 3 operations (loading+patching+saving) with one.
   */
  async patch(id: string, patch: Partial<BM>, opt: CommonDaoSaveOptions = {}): Promise<Saved<BM>> {
    return await this.save(
      {
        ...(await this.getByIdOrCreate(id, patch as BM, opt)),
        ...patch,
      } as BM,
      opt,
    )
  }

  async patchAsDBM(id: string, patch: Partial<DBM>, opt: CommonDaoSaveOptions = {}): Promise<DBM> {
    const dbm =
      (await this.getByIdAsDBM(id, opt)) ||
      ((this.create({ ...patch, id } as BM, opt) as any) as DBM)

    return await this.saveAsDBM(
      {
        ...dbm,
        ...patch,
      },
      opt,
    )
  }

  async saveAsDBM(dbm: DBM, opt: CommonDaoSaveOptions = {}): Promise<DBM> {
    this.requireWriteAccess()
    // assigning id in case it misses the id
    // will override/set `updated` field, unless opts.preserveUpdated is set
    if (!opt.raw) {
      dbm = this.assignIdCreatedUpdated(dbm, opt) as DBM
    }
    const op = `saveAsDBM(${dbm.id})`
    const table = opt.table || this.cfg.table
    const started = this.logSaveStarted(op, dbm, table)
    await this.cfg.db.saveBatch(table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    this.logSaveResult(started, op, table)
    return dbm
  }

  async saveBatch(bms: BM[], opt: CommonDaoSaveOptions = {}): Promise<Saved<BM>[]> {
    this.requireWriteAccess()
    const dbms = await this.bmsToDBM(bms, opt)
    const op = `saveBatch ${dbms.length} row(s) (${_truncate(
      dbms
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
    const table = opt.table || this.cfg.table
    const started = this.logSaveStarted(op, bms, table)
    await this.cfg.db.saveBatch(table, dbms, {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    const savedBMs = await this.dbmsToBM(dbms, opt)
    this.logSaveResult(started, op, table)
    return savedBMs
  }

  async saveBatchAsDBM(dbms: DBM[], opt: CommonDaoSaveOptions = {}): Promise<DBM[]> {
    this.requireWriteAccess()
    if (!opt.raw) {
      dbms = this.anyToDBMs(dbms, opt)
    }
    const op = `saveBatchAsDBM ${dbms.length} row(s) (${_truncate(
      dbms
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
    const table = opt.table || this.cfg.table
    const started = this.logSaveStarted(op, dbms, table)

    await this.cfg.db.saveBatch(table, dbms, {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })

    this.logSaveResult(started, op, table)
    return dbms
  }

  // DELETE
  /**
   * @returns number of deleted items
   */
  async deleteById(id?: string, opt: CommonDaoOptions = {}): Promise<number> {
    if (!id) return 0
    this.requireWriteAccess()
    const op = `deleteById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const ids = await this.cfg.db.deleteByIds(table, [id])
    this.logSaveResult(started, op, table)
    return ids
  }

  async deleteByIds(ids: string[], opt: CommonDaoOptions = {}): Promise<number> {
    this.requireWriteAccess()
    const op = `deleteByIds(${ids.join(', ')})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const deletedIds = await this.cfg.db.deleteByIds(table, ids)
    this.logSaveResult(started, op, table)
    return deletedIds
  }

  async deleteByQuery(q: DBQuery, opt?: CommonDaoOptions): Promise<number> {
    this.requireWriteAccess()
    const op = `deleteByQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const ids = await this.cfg.db.deleteByQuery(q, opt)
    this.logSaveResult(started, op, q.table)
    return ids
  }

  // CONVERSIONS

  async dbmToBM(_dbm: DBM, opt: CommonDaoOptions = {}): Promise<Saved<BM>> {
    if (!_dbm) return undefined as any

    const dbm = this.anyToDBM(_dbm, opt)

    // DBM > BM
    const bm = await this.beforeDBMToBM(dbm)

    // Validate/convert BM
    return this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  async dbmsToBM(dbms: DBM[], opt: CommonDaoOptions = {}): Promise<Saved<BM>[]> {
    return await Promise.all(dbms.map(dbm => this.dbmToBM(dbm, opt)))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  async bmToDBM(bm: BM, opt?: CommonDaoOptions): Promise<DBM> {
    if (bm === undefined) return undefined as any

    // Validate/convert BM
    // bm gets assigned to the new reference
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // BM > DBM
    let dbm = await this.beforeBMToDBM(bm)

    // Does not mutate
    dbm = this.assignIdCreatedUpdated(dbm, opt) as DBM

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opt)
  }

  async bmsToDBM(bms: BM[], opt: CommonDaoOptions = {}): Promise<DBM[]> {
    // try/catch?
    return await Promise.all(bms.map(bm => this.bmToDBM(bm, opt)))
  }

  anyToDBM(dbm: DBM, opt: CommonDaoOptions = {}): DBM {
    if (!dbm) return undefined as any

    dbm = { ...dbm, ...this.parseNaturalId(dbm.id) }

    if (opt.anonymize) {
      dbm = this.anonymize(dbm)
    }

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opt)
  }

  anyToDBMs(entities: DBM[], opt: CommonDaoOptions = {}): DBM[] {
    return entities.map(entity => this.anyToDBM(entity, opt))
  }

  async bmToTM(bm: Saved<BM>, opt?: CommonDaoOptions): Promise<TM> {
    if (bm === undefined) return undefined as any

    // Validate/convert BM
    // bm gets assigned to the new reference
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // BM > TM
    const tm = await this.beforeBMToTM(bm)

    // Validate/convert DBM
    return this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)
  }

  async bmsToTM(bms: Saved<BM>[], opt: CommonDaoOptions = {}): Promise<TM[]> {
    // try/catch?
    return await Promise.all(bms.map(bm => this.bmToTM(bm, opt)))
  }

  async tmToBM(tm: TM, opt: CommonDaoOptions = {}): Promise<BM> {
    if (!tm) return undefined as any

    // Validate/convert TM
    // bm gets assigned to the new reference
    tm = this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)

    // TM > BM
    const bm = await this.beforeTMToBM(tm)

    // Validate/convert BM
    return this.validateAndConvert<BM>(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  async tmsToBM(tms: TM[], opt: CommonDaoOptions = {}): Promise<BM[]> {
    // try/catch?
    return await Promise.all(tms.map(tm => this.tmToBM(tm, opt)))
  }

  /**
   * Returns *converted value*.
   * Validates (unless `skipValidation=true` passed).
   * Throws only if `throwOnError=true` passed OR if `env().throwOnEntityValidationError`
   */
  validateAndConvert<IN = any, OUT = IN>(
    obj: IN,
    schema?: ObjectSchemaTyped<IN>,
    modelType?: DBModelType,
    opt: CommonDaoOptions = {},
  ): OUT {
    // `raw` option completely bypasses any processing
    if (opt.raw) return (obj as any) as OUT

    // Pre-validation hooks
    if (modelType === DBModelType.DBM) {
      obj = this.beforeDBMValidate(obj as any) as any
    }

    // Return as is if no schema is passed or if `skipConversion` is set
    if (!schema || opt.skipConversion) {
      return obj as any
    }

    // This will Convert and Validate
    const table = opt.table || this.cfg.table
    const { value, error } = getValidationResult<IN, OUT>(obj, schema, table + (modelType || ''))

    // If we care about validation and there's an error
    if (error && !opt.skipValidation) {
      if (
        opt.throwOnError ||
        (this.cfg.throwOnEntityValidationError && opt.throwOnError === undefined)
      ) {
        throw error
      } else {
        // capture by Sentry and ignore the error
        // It will still *convert* the value and return.
        if (this.cfg.onValidationError) {
          this.cfg.onValidationError(error)
        }
      }
    }

    return value // converted value
  }

  async getTableSchema(): Promise<CommonSchema> {
    return this.cfg.db.getTableSchema<DBM>(this.cfg.table)
  }

  async createTable(schema: CommonSchema, opt?: CommonDaoCreateOptions): Promise<void> {
    this.requireWriteAccess()
    await this.cfg.db.createTable(schema, opt)
  }

  /**
   * Proxy to this.cfg.db.ping
   */
  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  protected logResult(started: number, op: string, res: any, table: string): void {
    if (!this.cfg.logLevel) return

    let logRes: any
    const args: any[] = []

    if (Array.isArray(res)) {
      logRes = `${res.length} row(s)`
      if (res.length && this.cfg.logLevel >= CommonDaoLogLevel.DATA_FULL) {
        args.push('\n', res.slice(0, 10)) // max 10 items
      }
    } else if (res) {
      logRes = `1 row`
      if (this.cfg.logLevel >= CommonDaoLogLevel.DATA_SINGLE) {
        args.push('\n', res)
      }
    } else {
      logRes = `undefined`
    }

    log(...[`<< ${table}.${op}: ${logRes} in ${since(started)}`].concat(args))
  }

  protected logSaveResult(started: number, op: string, table: string): void {
    if (!this.cfg.logLevel) return
    log(`<< ${table}.${op} in ${since(started)}`)
  }

  protected logStarted(op: string, table: string, force = false): number {
    if (this.cfg.logStarted || force) {
      log(`>> ${table}.${op}`)
    }
    return Date.now()
  }

  protected logSaveStarted(op: string, items: any, table: string): number {
    if (this.cfg.logStarted) {
      const args: any[] = [`>> ${table}.${op}`]
      if (Array.isArray(items)) {
        if (items.length && this.cfg.logLevel! >= CommonDaoLogLevel.DATA_FULL) {
          args.push('\n', ...items.slice(0, 10))
        } else {
          args.push(`${items.length} row(s)`)
        }
      } else {
        if (this.cfg.logLevel! >= CommonDaoLogLevel.DATA_SINGLE) {
          args.push(items)
        }
      }

      log(...args)
    }

    return Date.now()
  }
}
