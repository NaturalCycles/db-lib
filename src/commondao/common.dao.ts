import {
  _assert,
  _filterNullishValues,
  _filterUndefinedValues,
  _passthroughPredicate,
  _since,
  _truncate,
  _uniqBy,
  AppError,
  AsyncMapper,
  ErrorMode,
  JsonSchemaObject,
  JsonSchemaRootObject,
  ObjectWithId,
  pMap,
  pTimeout,
  Saved,
} from '@naturalcycles/js-lib'
import {
  _pipeline,
  AjvSchema,
  AjvValidationError,
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
  ReadableTyped,
  stringId,
  transformBuffer,
  transformLogProgress,
  transformMap,
  transformMapSimple,
  transformMapSync,
  transformTap,
  writableVoid,
} from '@naturalcycles/nodejs-lib'
import { DBLibError } from '../cnst'
import { DBModelType, RunQueryResult } from '../db.model'
import { DBQuery, RunnableDBQuery } from '../query/dbQuery'
import {
  CommonDaoCfg,
  CommonDaoCreateOptions,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
} from './common.dao.model'

/* eslint-disable no-dupe-class-members */

const isGAE = !!process.env['GAE_INSTANCE']
const isCI = !!process.env['CI']

/**
 * Lowest common denominator API between supported Databases.
 *
 * DBM = Database model (how it's stored in DB)
 * BM = Backend model (optimized for API access)
 * TM = Transport model (optimized to be sent over the wire)
 */
export class CommonDao<
  BM extends Partial<ObjectWithId>,
  DBM extends ObjectWithId = Saved<BM>,
  TM = BM,
> {
  constructor(public cfg: CommonDaoCfg<BM, DBM, TM>) {
    this.cfg = {
      // Default is to NOT log in AppEngine and in CI,
      // otherwise to log Operations
      // e.g in Dev (local machine), Test - it will log operations (useful for debugging)
      logLevel: isGAE || isCI ? CommonDaoLogLevel.NONE : CommonDaoLogLevel.OPERATIONS,
      created: true,
      updated: true,
      logger: console,
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
        onValidationError: err => err,
        ...cfg.hooks,
      },
    }
  }

  // CREATE
  create(part: Partial<BM> = {}, opt: CommonDaoOptions = {}): Saved<BM> {
    let bm = this.cfg.hooks!.beforeCreate!(part) as BM
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // If no SCHEMA - return as is
    return this.assignIdCreatedUpdated(bm, opt)
  }

  // GET
  async getById(id: undefined, opt?: CommonDaoOptions): Promise<null>
  async getById(id?: string, opt?: CommonDaoOptions): Promise<Saved<BM> | null>
  async getById(id?: string, opt: CommonDaoOptions = {}): Promise<Saved<BM> | null> {
    if (!id) return null
    const op = `getById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)

    let dbm: DBM | undefined

    if (opt.timeout) {
      // todo: possibly remove it after debugging is done
      dbm = (
        await pTimeout(this.cfg.db.getByIds<DBM>(table, [id]), {
          timeout: opt.timeout,
          name: `getById(${table})`,
        })
      )[0]
    } else {
      dbm = (await this.cfg.db.getByIds<DBM>(table, [id]))[0]
    }

    const bm = opt.raw ? (dbm as any) : await this.dbmToBM(dbm, opt)
    this.logResult(started, op, bm, table)
    return bm || null
  }

  async getByIdOrEmpty(
    id: string,
    part: Partial<BM> = {},
    opt?: CommonDaoOptions,
  ): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ ...part, id }, opt)
  }

  async getByIdAsDBMOrEmpty(
    id: string,
    part: Partial<BM> = {},
    opt?: CommonDaoOptions,
  ): Promise<DBM> {
    const dbm = await this.getByIdAsDBM(id, opt)
    if (dbm) return dbm

    const bm: BM = this.create({ ...part, id }, opt) as any
    return await this.bmToDBM(bm, opt)
  }

  async getByIdAsDBM(id: undefined, opt?: CommonDaoOptions): Promise<null>
  async getByIdAsDBM(id?: string, opt?: CommonDaoOptions): Promise<DBM | null>
  async getByIdAsDBM(id?: string, opt: CommonDaoOptions = {}): Promise<DBM | null> {
    if (!id) return null
    const op = `getByIdAsDBM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (!opt.raw) {
      dbm = this.anyToDBM(dbm!, opt)
    }
    this.logResult(started, op, dbm, table)
    return dbm || null
  }

  async getByIdAsTM(id: undefined, opt?: CommonDaoOptions): Promise<null>
  async getByIdAsTM(id?: string, opt?: CommonDaoOptions): Promise<TM | null>
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
    const bm = await this.dbmToBM(dbm, opt)
    const tm = this.bmToTM(bm, opt)
    this.logResult(started, op, tm, table)
    return tm || null
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

  /**
   * Throws if readOnly is true
   */
  private requireObjectMutability(): void {
    if (this.cfg.immutable) {
      throw new AppError(DBLibError.OBJECT_IS_IMMUTABLE, {
        code: DBLibError.OBJECT_IS_IMMUTABLE,
        table: this.cfg.table,
      })
    }
  }

  async getBy(by: keyof DBM, value: any, limit = 0, opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.query().filterEq(by, value).limit(limit).runQuery(opt)
  }

  async getOneBy(by: keyof DBM, value: any, opt?: CommonDaoOptions): Promise<Saved<BM> | null> {
    const [bm] = await this.query().filterEq(by, value).limit(1).runQuery(opt)
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

  async runQuery(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    const { rows } = await this.runQueryExtended(q, opt)
    return rows
  }

  async runQuerySingleColumn<T = any>(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<T[]> {
    _assert(
      q._selectedFieldNames?.length === 1,
      `runQuerySingleColumn requires exactly 1 column to be selected: ${q.pretty()}`,
    )

    const col = q._selectedFieldNames[0]!

    const { rows } = await this.runQueryExtended(q, opt)
    return rows.map(r => r[col as any])
  }

  /**
   * Convenience method that runs multiple queries in parallel and then merges their results together.
   * Does deduplication by id.
   * Order is not guaranteed, as queries run in parallel.
   */
  async runUnionQueries(queries: DBQuery<DBM>[], opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    const results = (
      await pMap(queries, async q => (await this.runQueryExtended(q, opt)).rows)
    ).flat()
    return _uniqBy(results, r => r.id)
  }

  async runQueryExtended(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<Saved<BM>>> {
    q.table = opt.table || q.table
    const op = `runQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const bms = partialQuery || opt.raw ? (rows as any[]) : await this.dbmsToBM(rows, opt)
    this.logResult(started, op, bms, q.table)
    return {
      rows: bms,
      ...queryResult,
    }
  }

  async runQueryAsDBM(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<DBM[]> {
    const { rows } = await this.runQueryExtendedAsDBM(q, opt)
    return rows
  }

  async runQueryExtendedAsDBM(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<DBM>> {
    q.table = opt.table || q.table
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const dbms = partialQuery || opt.raw ? rows : this.anyToDBMs(rows, opt)
    this.logResult(started, op, dbms, q.table)
    return { rows: dbms, ...queryResult }
  }

  async runQueryAsTM(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<TM[]> {
    const { rows } = await this.runQueryExtendedAsTM(q, opt)
    return rows
  }

  async runQueryExtendedAsTM(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<TM>> {
    q.table = opt.table || q.table
    const op = `runQueryAsTM(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const tms =
      partialQuery || opt.raw ? (rows as any[]) : this.bmsToTM(await this.dbmsToBM(rows, opt), opt)
    this.logResult(started, op, tms, q.table)
    return {
      rows: tms,
      ...queryResult,
    }
  }

  async runQueryCount(q: DBQuery<DBM>, opt: CommonDaoOptions = {}): Promise<number> {
    q.table = opt.table || q.table
    const op = `runQueryCount(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const count = await this.cfg.db.runQueryCount(q, opt)
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      this.cfg.logger?.log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
    return count
  }

  async streamQueryForEach(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<Saved<BM>, void>,
    opt: CommonDaoStreamForEachOptions<Saved<BM>> = {},
  ): Promise<void> {
    q.table = opt.table || q.table
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.skipConversion = opt.skipConversion !== false // default true
    opt.errorMode ||= ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM>(q, opt),
      // optimization: 1 validation is enough
      // transformMap<any, DBM>(dbm => (partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)), opt),
      transformMap<DBM, Saved<BM>>(
        async dbm => {
          count++
          return partialQuery || opt.raw ? (dbm as any) : await this.dbmToBM(dbm, opt)
        },
        {
          errorMode: opt.errorMode,
        },
      ),
      transformMap<Saved<BM>, void>(mapper, {
        ...opt,
        predicate: _passthroughPredicate, // to be able to logProgress
      }),
      // LogProgress should be AFTER the mapper, to be able to report correct stats
      transformLogProgress({
        metric: q.table,
        ...opt,
      }),
      writableVoid(),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      this.cfg.logger?.log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
  }

  async streamQueryAsDBMForEach(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<DBM, void>,
    opt: CommonDaoStreamForEachOptions<DBM> = {},
  ): Promise<void> {
    q.table = opt.table || q.table
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.skipConversion = opt.skipConversion !== false // default true
    opt.errorMode ||= ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames
    const op = `streamQueryAsDBMForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<any>(q, opt),
      transformMapSync<any, DBM>(
        dbm => {
          count++
          return partialQuery || opt.raw ? dbm : this.anyToDBM(dbm, opt)
        },
        {
          errorMode: opt.errorMode,
        },
      ),
      transformMap<DBM, void>(mapper, {
        ...opt,
        predicate: _passthroughPredicate, // to be able to logProgress
      }),
      // LogProgress should be AFTER the mapper, to be able to report correct stats
      transformLogProgress({
        metric: q.table,
        ...opt,
      }),
      writableVoid(),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      this.cfg.logger?.log(`<< ${q.table}.${op}: ${count} row(s) in ${_since(started)}`)
    }
  }

  /**
   * Stream as Readable, to be able to .pipe() it further with support of backpressure.
   */
  streamQueryAsDBM(q: DBQuery<DBM>, opt: CommonDaoStreamOptions = {}): ReadableTyped<DBM> {
    q.table = opt.table || q.table
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.skipConversion = opt.skipConversion !== false // default true
    opt.errorMode ||= ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames

    const stream = this.cfg.db.streamQuery<DBM>(q, opt)
    if (partialQuery || opt.raw) return stream

    return stream.pipe(
      transformMapSimple<any, DBM>(dbm => this.anyToDBM(dbm, opt), {
        errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
      }),
    )
  }

  /**
   * Stream as Readable, to be able to .pipe() it further with support of backpressure.
   *
   * Please note that this stream is currently not async-iteration friendly, because of
   * `through2` usage.
   * Will be migrated/fixed at some point in the future.
   *
   * You can do `.pipe(transformNoOp)` to make it "valid again".
   */
  streamQuery(q: DBQuery<DBM>, opt: CommonDaoStreamOptions = {}): ReadableTyped<Saved<BM>> {
    q.table = opt.table || q.table
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.skipConversion = opt.skipConversion !== false // default true
    opt.errorMode ||= ErrorMode.SUPPRESS

    const stream = this.cfg.db.streamQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    if (partialQuery || opt.raw) return stream

    return (
      stream
        // optimization: 1 validation is enough
        // .pipe(transformMap<any, DBM>(dbm => this.anyToDBM(dbm, opt), safeOpt))
        // .pipe(transformMap<DBM, Saved<BM>>(dbm => this.dbmToBM(dbm, opt), safeOpt))
        .pipe(
          transformMap<DBM, Saved<BM>>(async dbm => await this.dbmToBM(dbm, opt), {
            errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
          }),
        )
      // this can make the stream async-iteration-friendly
      // but not applying it now for perf reasons
      // .pipe(transformNoOp())
    )
  }

  async queryIds(q: DBQuery<DBM>, opt: CommonDaoOptions = {}): Promise<string[]> {
    q.table = opt.table || q.table
    const { rows } = await this.cfg.db.runQuery(q.select(['id']), opt)
    return rows.map(r => r.id)
  }

  streamQueryIds(q: DBQuery<DBM>, opt: CommonDaoStreamOptions = {}): ReadableTyped<string> {
    q.table = opt.table || q.table
    opt.errorMode ||= ErrorMode.SUPPRESS

    return this.cfg.db.streamQuery<DBM>(q.select(['id']), opt).pipe(
      transformMapSimple<ObjectWithId, string>(objectWithId => objectWithId.id, {
        errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
      }),
    )
  }

  async streamQueryIdsForEach(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<string, void>,
    opt: CommonDaoStreamForEachOptions<string> = {},
  ): Promise<void> {
    q.table = opt.table || q.table
    opt.errorMode = opt.errorMode || ErrorMode.SUPPRESS

    const op = `streamQueryIdsForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM>(q.select(['id']), opt),
      transformMapSimple<ObjectWithId, string>(objectWithId => objectWithId.id),
      transformTap(() => count++),
      transformMap<string, void>(mapper, {
        ...opt,
        predicate: _passthroughPredicate,
      }),
      // LogProgress should be AFTER the mapper, to be able to report correct stats
      transformLogProgress({
        metric: q.table,
        ...opt,
      }),
      writableVoid(),
    ])

    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      this.cfg.logger?.log(`<< ${q.table}.${op}: ${count} id(s) in ${_since(started)}`)
    }
  }

  /**
   * Mutates!
   * "Returns", just to have a type of "Saved"
   */
  assignIdCreatedUpdated(obj: DBM, opt?: CommonDaoOptions): DBM
  assignIdCreatedUpdated(obj: BM, opt?: CommonDaoOptions): Saved<BM>
  assignIdCreatedUpdated(obj: DBM | BM, opt: CommonDaoOptions = {}): DBM | Saved<BM> {
    const now = Math.floor(Date.now() / 1000)

    obj.id = obj.id || this.cfg.hooks!.createId!(obj)

    if (this.cfg.created) {
      Object.assign(obj, {
        created: (obj as any).created || (obj as any).updated || now,
      })
    }

    if (this.cfg.updated) {
      Object.assign(obj, {
        updated: opt.preserveUpdatedCreated && (obj as any).updated ? (obj as any).updated : now,
      })
    }

    return obj as any
  }

  // SAVE
  /**
   * Mutates with id, created, updated
   */
  async save(bm: BM, opt: CommonDaoSaveOptions<DBM> = {}): Promise<Saved<BM>> {
    this.requireWriteAccess()
    const idWasGenerated = !bm.id
    this.assignIdCreatedUpdated(bm, opt) // mutates
    const dbm = await this.bmToDBM(bm, opt)
    const table = opt.table || this.cfg.table
    if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, dbm)
    if (this.cfg.immutable) await this.ensureImmutableDoesntExist(table, dbm)
    const op = `save(${dbm.id})`
    const started = this.logSaveStarted(op, bm, table)
    await this.cfg.db.saveBatch(table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })

    this.logSaveResult(started, op, table)
    return bm as any
  }

  private async ensureImmutableDoesntExist(table: string, dbm: DBM): Promise<void> {
    await this.throwIfObjectExists(table, dbm, [
      DBLibError.OBJECT_IS_IMMUTABLE,
      {
        code: DBLibError.OBJECT_IS_IMMUTABLE,
        id: dbm.id,
        table,
      },
    ])
  }

  private async ensureUniqueId(table: string, dbm: DBM): Promise<void> {
    // todo: retry N times
    await this.throwIfObjectExists(table, dbm, [
      DBLibError.OBJECT_IS_IMMUTABLE,
      {
        code: DBLibError.OBJECT_IS_IMMUTABLE,
        id: dbm.id,
        table,
      },
    ])
  }

  private async throwIfObjectExists(
    table: string,
    dbm: DBM,
    errorMeta: [DBLibError, any],
  ): Promise<void> {
    const [existing] = await this.cfg.db.getByIds<DBM>(table, [dbm.id])
    if (existing) throw new AppError(errorMeta[0], errorMeta[1])
  }

  /**
   * Loads the row by id.
   * Creates the row (via this.create()) if it doesn't exist
   * (this will cause a validation error if Patch has not enough data for the row to be valid).
   * Saves (as fast as possible) with the Patch applied.
   *
   * Convenience method to replace 3 operations (loading+patching+saving) with one.
   */
  async patch(
    id: string,
    patch: Partial<BM>,
    opt: CommonDaoSaveOptions<DBM> = {},
  ): Promise<Saved<BM>> {
    return await this.save(
      {
        ...(await this.getByIdOrEmpty(id, patch, opt)),
        ...patch,
      } as any,
      opt,
    )
  }

  async patchAsDBM(
    id: string,
    patch: Partial<DBM>,
    opt: CommonDaoSaveOptions<DBM> = {},
  ): Promise<DBM> {
    const dbm =
      (await this.getByIdAsDBM(id, opt)) ||
      (this.create({ ...patch, id } as Partial<BM>, opt) as any as DBM)

    return await this.saveAsDBM(
      {
        ...dbm,
        ...patch,
      },
      opt,
    )
  }

  async saveAsDBM(dbm: DBM, opt: CommonDaoSaveOptions<DBM> = {}): Promise<DBM> {
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table

    // assigning id in case it misses the id
    // will override/set `updated` field, unless opts.preserveUpdated is set
    if (!opt.raw) {
      const idWasGenerated = !dbm.id
      this.assignIdCreatedUpdated(dbm, opt) // mutates
      dbm = this.anyToDBM(dbm, opt)
      if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, dbm)
      if (this.cfg.immutable) await this.ensureImmutableDoesntExist(table, dbm)
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

  async saveBatch(bms: BM[], opt: CommonDaoSaveOptions<DBM> = {}): Promise<Saved<BM>[]> {
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table
    bms.forEach(bm => this.assignIdCreatedUpdated(bm, opt))
    const dbms = await this.bmsToDBM(bms, opt)
    if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
    if (this.cfg.immutable)
      await pMap(dbms, async dbm => await this.ensureImmutableDoesntExist(table, dbm))

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

    return bms as any[]
  }

  async saveBatchAsDBM(dbms: DBM[], opt: CommonDaoSaveOptions<DBM> = {}): Promise<DBM[]> {
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table
    if (!opt.raw) {
      dbms.forEach(dbm => this.assignIdCreatedUpdated(dbm, opt)) // mutates
      dbms = this.anyToDBMs(dbms, opt)
      if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
      if (this.cfg.immutable)
        await pMap(dbms, async dbm => await this.ensureImmutableDoesntExist(table, dbm))
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
  async deleteById(id: undefined, opt?: CommonDaoOptions): Promise<0>
  async deleteById(id?: string, opt?: CommonDaoOptions): Promise<number>
  async deleteById(id?: string, opt: CommonDaoOptions = {}): Promise<number> {
    if (!id) return 0
    this.requireWriteAccess()
    if (!opt.allowMutability) this.requireObjectMutability()
    const op = `deleteById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const ids = await this.cfg.db.deleteByIds(table, [id])
    this.logSaveResult(started, op, table)
    return ids
  }

  async deleteByIds(ids: string[], opt: CommonDaoOptions = {}): Promise<number> {
    this.requireWriteAccess()
    if (!opt.allowMutability) this.requireObjectMutability()
    const op = `deleteByIds(${ids.join(', ')})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const deletedIds = await this.cfg.db.deleteByIds(table, ids)
    this.logSaveResult(started, op, table)
    return deletedIds
  }

  /**
   * Pass `stream: true` option to use Streaming: it will Stream the query, batch by 500, and execute
   * `deleteByIds` for each batch concurrently (infinite concurrency).
   * This is expected to be more memory-efficient way of deleting big numbers of rows.
   */
  async deleteByQuery(
    q: DBQuery<DBM>,
    opt: CommonDaoStreamForEachOptions<DBM> & { stream?: boolean } = {},
  ): Promise<number> {
    this.requireWriteAccess()
    if (!opt.allowMutability) this.requireObjectMutability()
    q.table = opt.table || q.table
    const op = `deleteByQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    let deleted = 0

    if (opt.stream) {
      const batchSize = 500

      await _pipeline([
        this.cfg.db.streamQuery<DBM>(q.select(['id']), opt),
        transformMapSimple<ObjectWithId, string>(objectWithId => objectWithId.id, {
          errorMode: ErrorMode.SUPPRESS,
        }),
        transformBuffer<string>({ batchSize }),
        transformMap<string[], void>(
          async ids => {
            deleted += await this.cfg.db.deleteByIds(q.table, ids, opt)
          },
          {
            predicate: _passthroughPredicate,
          },
        ),
        // LogProgress should be AFTER the mapper, to be able to report correct stats
        transformLogProgress({
          metric: q.table,
          logEvery: 2, // 500 * 2 === 1000
          batchSize,
          ...opt,
        }),
        writableVoid(),
      ])
    } else {
      deleted = await this.cfg.db.deleteByQuery(q, opt)
    }

    this.logSaveResult(started, op, q.table)
    return deleted
  }

  // CONVERSIONS

  async dbmToBM(_dbm: undefined, opt?: CommonDaoOptions): Promise<undefined>
  async dbmToBM(_dbm?: DBM, opt?: CommonDaoOptions): Promise<Saved<BM>>
  async dbmToBM(_dbm?: DBM, opt: CommonDaoOptions = {}): Promise<Saved<BM> | undefined> {
    if (!_dbm) return

    // optimization: no need to run full joi DBM validation, cause BM validation will be run
    // const dbm = this.anyToDBM(_dbm, opt)
    let dbm: DBM = { ..._dbm, ...this.cfg.hooks!.parseNaturalId!(_dbm.id) }

    if (opt.anonymize) {
      dbm = this.cfg.hooks!.anonymize!(dbm)
    }

    // DBM > BM
    const bm = await this.cfg.hooks!.beforeDBMToBM!(dbm)

    // Validate/convert BM
    // eslint-disable-next-line @typescript-eslint/return-await
    return this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  async dbmsToBM(dbms: DBM[], opt: CommonDaoOptions = {}): Promise<Saved<BM>[]> {
    return await pMap(dbms, async dbm => await this.dbmToBM(dbm, opt))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  async bmToDBM(bm: undefined, opt?: CommonDaoOptions): Promise<undefined>
  async bmToDBM(bm?: BM, opt?: CommonDaoOptions): Promise<DBM>
  async bmToDBM(bm?: BM, opt?: CommonDaoOptions): Promise<DBM | undefined> {
    if (bm === undefined) return

    // optimization: no need to run the BM validation, since DBM will be validated anyway
    // Validate/convert BM
    // bm gets assigned to the new reference
    // bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // should not do it on load, but only on save!
    // this.assignIdCreatedUpdated(bm, opt)

    // BM > DBM
    const dbm = { ...(await this.cfg.hooks!.beforeBMToDBM!(bm)) }

    // Validate/convert DBM
    // eslint-disable-next-line @typescript-eslint/return-await
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opt)
  }

  async bmsToDBM(bms: BM[], opt: CommonDaoOptions = {}): Promise<DBM[]> {
    // try/catch?
    return await pMap(bms, async bm => await this.bmToDBM(bm, opt))
  }

  anyToDBM(dbm: undefined, opt?: CommonDaoOptions): undefined
  anyToDBM(dbm?: any, opt?: CommonDaoOptions): DBM
  anyToDBM(dbm?: DBM, opt: CommonDaoOptions = {}): DBM | undefined {
    if (!dbm) return

    // this shouldn't be happening on load! but should on save!
    // this.assignIdCreatedUpdated(dbm, opt)

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

  bmToTM(bm: undefined, opt?: CommonDaoOptions): TM | undefined
  bmToTM(bm?: Saved<BM>, opt?: CommonDaoOptions): TM
  bmToTM(bm?: Saved<BM>, opt?: CommonDaoOptions): TM | undefined {
    if (bm === undefined) return

    // optimization: 1 validation is enough
    // Validate/convert BM
    // bm gets assigned to the new reference
    // bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // BM > TM
    const tm = this.cfg.hooks!.beforeBMToTM!(bm as any)

    // Validate/convert DBM
    return this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)
  }

  bmsToTM(bms: Saved<BM>[], opt: CommonDaoOptions = {}): TM[] {
    // try/catch?
    return bms.map(bm => this.bmToTM(bm, opt))
  }

  tmToBM(tm: undefined, opt?: CommonDaoOptions): undefined
  tmToBM(tm?: TM, opt?: CommonDaoOptions): BM
  tmToBM(tm?: TM, opt: CommonDaoOptions = {}): BM | undefined {
    if (!tm) return

    // optimization: 1 validation is enough
    // Validate/convert TM
    // bm gets assigned to the new reference
    // tm = this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)

    // TM > BM
    const bm = this.cfg.hooks!.beforeTMToBM!(tm) as BM

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
   *
   * Does NOT mutate the object.
   */
  validateAndConvert<IN, OUT = IN>(
    obj: Partial<IN>,
    schema: ObjectSchemaTyped<IN> | AjvSchema<IN> | undefined,
    modelType: DBModelType,
    opt: CommonDaoOptions = {},
  ): OUT {
    // `raw` option completely bypasses any processing
    if (opt.raw) return obj as any as OUT

    // Kirill 2021-10-18: I realized that there's little reason to keep removing null values
    // So, from now on we'll preserve them
    // "undefined" values, I believe, are/were not saved to/from DB anyway (due to e.g JSON.stringify removing them)
    // But let's keep watching it!
    //
    // Filter null and undefined values
    // obj = _filterNullishValues(obj as any)
    // We still filter `undefined` values here, because `beforeDBMToBM` can return undefined values
    // and they can be annoying with snapshot tests
    if (this.cfg.filterNullishValues) {
      obj = _filterNullishValues(obj)
    } else {
      obj = _filterUndefinedValues(obj)
    }

    // Pre-validation hooks
    if (modelType === DBModelType.DBM) {
      obj = this.cfg.hooks!.beforeDBMValidate!(obj as any) as IN
    }

    // Return as is if no schema is passed or if `skipConversion` is set
    if (!schema || opt.skipConversion) {
      return obj as OUT
    }

    // This will Convert and Validate
    const table = opt.table || this.cfg.table
    const objectName = table + (modelType || '')

    let error: JoiValidationError | AjvValidationError | undefined
    let convertedValue: any

    if (schema instanceof AjvSchema) {
      // Ajv schema
      convertedValue = obj // because Ajv mutates original object

      error = schema.getValidationError(obj as IN, {
        objectName,
      })
    } else {
      // Joi
      const vr = getValidationResult<IN, OUT>(obj as IN, schema, objectName)
      error = vr.error
      convertedValue = vr.value
    }

    // If we care about validation and there's an error
    if (error && !opt.skipValidation) {
      const processedError = this.cfg.hooks!.onValidationError!(error)

      if (processedError) throw processedError
    }

    return convertedValue
  }

  async getTableSchema(): Promise<JsonSchemaRootObject<DBM>> {
    return await this.cfg.db.getTableSchema<DBM>(this.cfg.table)
  }

  async createTable(schema: JsonSchemaObject<DBM>, opt?: CommonDaoCreateOptions): Promise<void> {
    this.requireWriteAccess()
    await this.cfg.db.createTable(this.cfg.table, schema as any, opt)
  }

  /**
   * Proxy to this.cfg.db.ping
   */
  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

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
        args.push('\n', ...res.slice(0, 10)) // max 10 items
      }
    } else if (res) {
      logRes = `1 row`
      if (this.cfg.logLevel >= CommonDaoLogLevel.DATA_SINGLE) {
        args.push('\n', res)
      }
    } else {
      logRes = `undefined`
    }

    this.cfg.logger?.log(`<< ${table}.${op}: ${logRes} in ${_since(started)}`, ...args)
  }

  protected logSaveResult(started: number, op: string, table: string): void {
    if (!this.cfg.logLevel) return
    this.cfg.logger?.log(`<< ${table}.${op} in ${_since(started)}`)
  }

  protected logStarted(op: string, table: string, force = false): number {
    if (this.cfg.logStarted || force) {
      this.cfg.logger?.log(`>> ${table}.${op}`)
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

      this.cfg.logger?.log(...args)
    }

    return Date.now()
  }
}
