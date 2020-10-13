import {
  AppError,
  AsyncMapper,
  ErrorMode,
  _filterNullishValues,
  _since,
  _truncate,
} from '@naturalcycles/js-lib'
import {
  Debug,
  getValidationResult,
  ObjectSchemaTyped,
  ReadableTyped,
  stringId,
  transformLogProgress,
  transformMap,
  transformTap,
  writableForEach,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import {
  BaseDBEntity,
  DBModelType,
  ObjectWithId,
  RunQueryResult,
  Saved,
  SavedDBEntity,
} from '../db.model'
import { DBLibError } from '../index'
import { DBQuery, RunnableDBQuery } from '../query/dbQuery'
import { CommonSchema } from '../schema/common.schema'
import {
  CommonDaoCfg,
  CommonDaoCreateOptions,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
} from './common.dao.model'

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
      hooks: {
        createId: () => stringId(),
        parseNaturalId: () => ({}),
        beforeCreate: bm => bm as BM,
        beforeDBMValidate: dbm => dbm,
        beforeDBMToBM: dbm => dbm as any,
        beforeBMToDBM: bm => bm as any,
        beforeTMToBM: tm => tm as any,
        beforeBMToTM: bm => bm as any,
        anonymize: dbm => dbm,
        ...cfg.hooks,
      },
    }
  }

  // CREATE
  create(input: Partial<BM>, opt: CommonDaoOptions = {}): Saved<BM> {
    if (opt.throwOnError === undefined) {
      opt.throwOnError = this.cfg.throwOnDaoCreateObject
    }

    let bm = this.cfg.hooks!.beforeCreate!(input)
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // If no SCHEMA - return as is
    return this.assignIdCreatedUpdated(bm, opt)
  }

  // GET
  async getById(id?: string, opt: CommonDaoOptions = {}): Promise<Saved<BM> | null> {
    if (!id) return null
    const op = `getById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    const bm = opt.raw ? (dbm as any) : this.dbmToBM(dbm, opt)
    this.logResult(started, op, bm, table)
    return bm || null
  }

  async getByIdOrCreate(
    id: string,
    bmToCreate: Partial<BM>,
    opt?: CommonDaoOptions,
  ): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ ...bmToCreate, id }, opt)
  }

  async getByIdOrEmpty(id: string, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ id } as Partial<BM>, opt)
  }

  async getByIdAsDBMOrEmpty(id: string, opt?: CommonDaoOptions): Promise<DBM> {
    const dbm = await this.getByIdAsDBM(id, opt)
    if (dbm) return dbm

    return this.create({ id } as Partial<BM>, opt) as any
  }

  async getByIdAsDBM(id?: string, opt: CommonDaoOptions = {}): Promise<DBM | null> {
    if (!id) return null
    const op = `getByIdAsDBM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (!opt.raw) {
      dbm = this.anyToDBM(dbm, opt)
    }
    this.logResult(started, op, dbm, table)
    return dbm || null
  }

  async getByIdAsTM(id?: string, opt: CommonDaoOptions = {}): Promise<TM | null> {
    if (!id) return null
    const op = `getByIdAsTM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (opt.raw) {
      this.logResult(started, op, dbm, table)
      return (dbm as any) || null
    }
    const bm = this.dbmToBM(dbm, opt)
    const tm = this.bmToTM(bm, opt)
    this.logResult(started, op, tm, table)
    return tm || null
  }

  async getByIds(ids: string[], opt: CommonDaoOptions = {}): Promise<Saved<BM>[]> {
    const op = `getByIds ${ids.length} id(s) (${_truncate(ids.slice(0, 10).join(', '), 50)})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const dbms = await this.cfg.db.getByIds<DBM>(table, ids)
    const bms = opt.raw ? (dbms as any) : this.dbmsToBM(dbms, opt)
    this.logResult(started, op, bms, table)
    return bms
  }

  async getByIdsAsDBM(ids: string[], opt: CommonDaoOptions = {}): Promise<DBM[]> {
    const op = `getByIdsAsDBM ${ids.length} id(s) (${_truncate(ids.slice(0, 10).join(', '), 50)})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const dbms = await this.cfg.db.getByIds<DBM>(table, ids)
    this.logResult(started, op, dbms, table)
    return dbms
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
    throw new AppError(`DB row required, but not found: ${table}.${id}`, {
      code: DBLibError.DB_ROW_REQUIRED,
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

  async getOneBy(by: string, value: any, opt?: CommonDaoOptions): Promise<Saved<BM> | null> {
    const [bm] = await this.query().filter(by, '=', value).limit(1).runQuery(opt)
    return bm || null
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

  async runQuery<OUT = Saved<BM>>(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<OUT[]> {
    const { rows } = await this.runQueryExtended<OUT>(q, opt)
    return rows
  }

  async runQueryExtended<OUT = Saved<BM>>(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<OUT>> {
    const op = `runQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const bms = partialQuery || opt.raw ? (rows as any[]) : this.dbmsToBM(rows, opt)
    this.logResult(started, op, bms, q.table)
    return {
      rows: bms,
      ...queryResult,
    }
  }

  async runQueryAsDBM<OUT = DBM>(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<OUT[]> {
    const { rows } = await this.runQueryExtendedAsDBM<OUT>(q, opt)
    return rows
  }

  async runQueryExtendedAsDBM<OUT = DBM>(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<OUT>> {
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const dbms = partialQuery || opt.raw ? rows : this.anyToDBMs(rows, opt)
    this.logResult(started, op, dbms, q.table)
    return { rows: (dbms as any) as OUT[], ...queryResult }
  }

  async runQueryCount(q: DBQuery<DBM>, opt: CommonDaoOptions = {}): Promise<number> {
    const op = `runQueryCount(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const count = await this.cfg.db.runQueryCount(q, opt)
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
    return count
  }

  async streamQueryForEach<IN = Saved<BM>, OUT = IN>(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<OUT, void>,
    opt: CommonDaoStreamForEachOptions = {},
  ): Promise<void> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM>(q, opt),
      // optimization: 1 validation is enough
      // transformMap<any, DBM>(dbm => (partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)), opt),
      transformMap<DBM, Saved<BM>>(
        dbm => (partialQuery || opt.raw ? (dbm as any) : this.dbmToBM(dbm, opt)),
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
      log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
  }

  async streamQueryAsDBMForEach<IN = DBM, OUT = IN>(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<OUT, void>,
    opt: CommonDaoStreamForEachOptions = {},
  ): Promise<void> {
    if (opt.skipValidation === undefined) opt.skipValidation = true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryAsDBMForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<any>(q, opt),
      transformMap<any, DBM>(dbm => (partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)), opt),
      transformTap(() => count++),
      transformLogProgress<DBM>({
        metric: q.table,
        ...opt,
      }),
      writableForEach<OUT>(mapper, opt),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
  }

  /**
   * Stream as Readable, to be able to .pipe() it further with support of backpressure.
   */
  streamQueryAsDBM<OUT = DBM>(
    q: DBQuery<DBM>,
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
    q: DBQuery<DBM>,
    opt: CommonDaoStreamOptions = {},
  ): ReadableTyped<OUT> {
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const stream = this.cfg.db.streamQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    if (partialQuery || opt.raw) return stream

    const safeOpt = {
      ...opt,
      errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
    }

    return (
      stream
        // optimization: 1 validation is enough
        // .pipe(transformMap<any, DBM>(dbm => this.anyToDBM(dbm, opt), safeOpt))
        .pipe(transformMap<DBM, Saved<BM>>(dbm => this.dbmToBM(dbm, opt), safeOpt))
    )
  }

  async queryIds(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<string[]> {
    const { rows } = await this.cfg.db.runQuery<DBM, ObjectWithId>(q.select(['id']), opt)
    return rows.map(r => r.id)
  }

  streamQueryIds(q: DBQuery<DBM>, opt: CommonDaoStreamOptions = {}): ReadableTyped<string> {
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
    q: DBQuery<DBM>,
    mapper: AsyncMapper<string, void>,
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
      log(`<< ${q.table}.${op}: ${count} id(s) in ${_since(started)}`)
    }
  }

  /**
   * Mutates!
   * "Returns", just to have a type of "Saved"
   */
  assignIdCreatedUpdated<T extends DBM | BM>(obj: T, opt: CommonDaoOptions = {}): Saved<T> {
    const now = Math.floor(Date.now() / 1000)

    return Object.assign(obj, {
      id: obj.id || this.cfg.hooks!.createId!(obj),
      created: obj.created || obj.updated || now,
      updated: opt.preserveUpdatedCreated && obj.updated ? obj.updated : now,
    })
  }

  // SAVE
  /**
   * Mutates with id, created, updated
   */
  async save(bm: BM, opt: CommonDaoSaveOptions = {}): Promise<Saved<BM>> {
    this.requireWriteAccess()
    const dbm = this.bmToDBM(bm, opt) // does assignIdCreatedUpdated, mutates
    const table = opt.table || this.cfg.table
    const idWasGenerated = !bm.id
    if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, dbm)
    const op = `save(${dbm.id})`
    const started = this.logSaveStarted(op, bm, table)
    await this.cfg.db.saveBatch(table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })

    this.logSaveResult(started, op, table)
    return bm as Saved<BM>
  }

  /**
   * Mutates id if needed
   */
  private async ensureUniqueId(table: string, dbm: DBM): Promise<void> {
    // todo: retry N times
    const [existing] = await this.cfg.db.getByIds<DBM>(table, [dbm.id])
    if (existing) {
      throw new AppError(DBLibError.NON_UNIQUE_ID, {
        code: DBLibError.NON_UNIQUE_ID,
        id: dbm.id,
        table,
      })
    }
  }

  /**
   * Loads the row by id.
   * Creates the row (via this.create()) if it doesn't exist
   * (this will cause a validation error if Patch has not enough data for the row to be valid).
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
      ((this.create({ ...patch, id } as Partial<BM>, opt) as any) as DBM)

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
    const table = opt.table || this.cfg.table

    // assigning id in case it misses the id
    // will override/set `updated` field, unless opts.preserveUpdated is set
    if (!opt.raw) {
      const idWasGenerated = !dbm.id
      this.assignIdCreatedUpdated(dbm, opt)
      if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, dbm)
    }
    const op = `saveAsDBM(${dbm.id})`
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
    const table = opt.table || this.cfg.table
    const dbms = this.bmsToDBM(bms, opt) // does assignIdCreatedUpdated (mutates)
    if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
    const op = `saveBatch ${dbms.length} row(s) (${_truncate(
      dbms
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
    const started = this.logSaveStarted(op, bms, table)

    await this.cfg.db.saveBatch(table, dbms, {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })

    this.logSaveResult(started, op, table)

    return bms as Saved<BM>[]
  }

  async saveBatchAsDBM(dbms: DBM[], opt: CommonDaoSaveOptions = {}): Promise<DBM[]> {
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table
    if (!opt.raw) {
      dbms = this.anyToDBMs(dbms, opt) // does assignIdCreatedUpdated
      if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
    }
    const op = `saveBatchAsDBM ${dbms.length} row(s) (${_truncate(
      dbms
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
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

  dbmToBM(_dbm: DBM, opt: CommonDaoOptions = {}): Saved<BM> {
    if (!_dbm) return undefined as any

    // optimization: no need to run full joi DBM validation, cause BM validation will be run
    // const dbm = this.anyToDBM(_dbm, opt)
    let dbm: DBM = { ..._dbm, ...this.cfg.hooks!.parseNaturalId!(_dbm.id) }

    if (opt.anonymize) {
      dbm = this.cfg.hooks!.anonymize!(dbm)
    }

    // DBM > BM
    const bm = this.cfg.hooks!.beforeDBMToBM!(dbm)

    // Validate/convert BM
    return this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  dbmsToBM(dbms: DBM[], opt: CommonDaoOptions = {}): Saved<BM>[] {
    return dbms.map(dbm => this.dbmToBM(dbm, opt))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  bmToDBM(bm: BM, opt?: CommonDaoOptions): DBM {
    if (bm === undefined) return undefined as any

    // optimization: no need to run the BM validation, since DBM will be validated anyway
    // Validate/convert BM
    // bm gets assigned to the new reference
    // bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // Mutates
    this.assignIdCreatedUpdated(bm, opt)

    // BM > DBM
    const dbm = { ...this.cfg.hooks!.beforeBMToDBM!(bm) }

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opt)
  }

  bmsToDBM(bms: BM[], opt: CommonDaoOptions = {}): DBM[] {
    // try/catch?
    return bms.map(bm => this.bmToDBM(bm, opt))
  }

  anyToDBM(dbm: DBM, opt: CommonDaoOptions = {}): DBM {
    if (!dbm) return undefined as any

    this.assignIdCreatedUpdated(dbm, opt) // mutates

    dbm = { ...dbm, ...this.cfg.hooks!.parseNaturalId!(dbm.id) }

    if (opt.anonymize) {
      dbm = this.cfg.hooks!.anonymize!(dbm)
    }

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opt)
  }

  anyToDBMs(entities: DBM[], opt: CommonDaoOptions = {}): DBM[] {
    return entities.map(entity => this.anyToDBM(entity, opt))
  }

  bmToTM(bm: Saved<BM>, opt?: CommonDaoOptions): TM {
    if (bm === undefined) return undefined as any

    // optimization: 1 validation is enough
    // Validate/convert BM
    // bm gets assigned to the new reference
    // bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // BM > TM
    const tm = this.cfg.hooks!.beforeBMToTM!(bm as BM)

    // Validate/convert DBM
    return this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)
  }

  bmsToTM(bms: Saved<BM>[], opt: CommonDaoOptions = {}): TM[] {
    // try/catch?
    return bms.map(bm => this.bmToTM(bm, opt))
  }

  tmToBM(tm: TM, opt: CommonDaoOptions = {}): BM {
    if (!tm) return undefined as any

    // optimization: 1 validation is enough
    // Validate/convert TM
    // bm gets assigned to the new reference
    // tm = this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)

    // TM > BM
    const bm = this.cfg.hooks!.beforeTMToBM!(tm)

    // Validate/convert BM
    return this.validateAndConvert<BM>(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  tmsToBM(tms: TM[], opt: CommonDaoOptions = {}): BM[] {
    // try/catch?
    return tms.map(tm => this.tmToBM(tm, opt))
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

    // Filter null and undefined values
    obj = _filterNullishValues(obj as any)

    // Pre-validation hooks
    if (modelType === DBModelType.DBM) {
      obj = this.cfg.hooks!.beforeDBMValidate!(obj as any) as any
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

  // todo: logging
  // todo: bmToDBM, etc. How?
  // transaction(): DBTransaction {
  //   return this.cfg.db.transaction()
  // }

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

    log(...[`<< ${table}.${op}: ${logRes} in ${_since(started)}`].concat(args))
  }

  protected logSaveResult(started: number, op: string, table: string): void {
    if (!this.cfg.logLevel) return
    log(`<< ${table}.${op} in ${_since(started)}`)
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
