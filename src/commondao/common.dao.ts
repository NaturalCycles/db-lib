import { Transform } from 'node:stream'
import {
  _assert,
  _deepJsonEquals,
  _filterNullishValues,
  _filterUndefinedValues,
  _isTruthy,
  _objectAssignExact,
  _passthroughPredicate,
  _since,
  _truncate,
  _uniqBy,
  AnyObject,
  AppError,
  AsyncMapper,
  BaseDBEntity,
  CommonLogger,
  ErrorMode,
  JsonSchemaObject,
  JsonSchemaRootObject,
  ObjectWithId,
  PartialObjectWithId,
  pMap,
  Saved,
  SKIP,
  UnixTimestampMillisNumber,
  ZodSchema,
  ZodValidationError,
  zSafeValidate,
} from '@naturalcycles/js-lib'
import {
  _pipeline,
  AjvSchema,
  AjvValidationError,
  getValidationResult,
  JoiValidationError,
  ObjectSchema,
  ReadableTyped,
  stringId,
  transformBuffer,
  transformLogProgress,
  transformMap,
  transformMapSimple,
  writableVoid,
} from '@naturalcycles/nodejs-lib'
import { DBLibError } from '../cnst'
import {
  CommonDBTransactionOptions,
  DBModelType,
  DBPatch,
  DBTransaction,
  RunQueryResult,
} from '../db.model'
import { DBQuery, RunnableDBQuery } from '../query/dbQuery'
import {
  CommonDaoCfg,
  CommonDaoCreateOptions,
  CommonDaoHooks,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoSaveBatchOptions,
  CommonDaoSaveOptions,
  CommonDaoStreamDeleteOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
  CommonDaoStreamSaveOptions,
} from './common.dao.model'

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
  BM extends PartialObjectWithId,
  DBM extends PartialObjectWithId = BM,
  TM extends AnyObject = BM,
> {
  constructor(public cfg: CommonDaoCfg<BM, DBM, TM>) {
    this.cfg = {
      // Default is to NOT log in AppEngine and in CI,
      // otherwise to log Operations
      // e.g in Dev (local machine), Test - it will log operations (useful for debugging)
      logLevel: isGAE || isCI ? CommonDaoLogLevel.NONE : CommonDaoLogLevel.OPERATIONS,
      createId: true,
      assignGeneratedIds: false,
      useCreatedProperty: true,
      useUpdatedProperty: true,
      logger: console,
      ...cfg,
      hooks: {
        parseNaturalId: () => ({}),
        beforeCreate: bm => bm as BM,
        beforeDBMValidate: dbm => dbm,
        beforeDBMToBM: dbm => dbm as any,
        beforeBMToDBM: bm => bm as any,
        beforeBMToTM: bm => bm as any,
        anonymize: dbm => dbm,
        onValidationError: err => err,
        ...cfg.hooks,
      } satisfies Partial<CommonDaoHooks<BM, DBM, TM>>,
    }

    if (this.cfg.createId) {
      this.cfg.hooks!.createRandomId ||= () => stringId()
    } else {
      delete this.cfg.hooks!.createRandomId
    }
  }

  // CREATE
  create(part: Partial<BM> = {}, opt: CommonDaoOptions = {}): Saved<BM> {
    const bm = this.cfg.hooks!.beforeCreate!(part)
    // First assignIdCreatedUpdated, then validate!
    this.assignIdCreatedUpdated(bm, opt)
    return this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  // GET
  async getById(id: undefined | null, opt?: CommonDaoOptions): Promise<null>
  async getById(id?: string | null, opt?: CommonDaoOptions): Promise<Saved<BM> | null>
  async getById(id?: string | null, opt: CommonDaoOptions = {}): Promise<Saved<BM> | null> {
    if (!id) return null
    const op = `getById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)

    let dbm = (await this.cfg.db.getByIds<DBM>(table, [id]))[0]
    if (dbm && !opt.raw && this.cfg.hooks!.afterLoad) {
      dbm = (await this.cfg.hooks!.afterLoad(dbm)) || undefined
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
  ): Promise<Saved<DBM>> {
    const dbm = await this.getByIdAsDBM(id, opt)
    if (dbm) return dbm

    const bm = this.create({ ...part, id }, opt)
    return await this.bmToDBM(bm, opt)
  }

  async getByIdAsDBM(id: undefined | null, opt?: CommonDaoOptions): Promise<null>
  async getByIdAsDBM(id?: string | null, opt?: CommonDaoOptions): Promise<Saved<DBM> | null>
  async getByIdAsDBM(id?: string | null, opt: CommonDaoOptions = {}): Promise<Saved<DBM> | null> {
    if (!id) return null
    const op = `getByIdAsDBM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (dbm && !opt.raw && this.cfg.hooks!.afterLoad) {
      dbm = (await this.cfg.hooks!.afterLoad(dbm)) || undefined
    }

    if (!opt.raw) {
      dbm = this.anyToDBM(dbm!, opt)
    }
    this.logResult(started, op, dbm, table)
    return dbm || null
  }

  async getByIdAsTM(id: undefined | null, opt?: CommonDaoOptions): Promise<null>
  async getByIdAsTM(id?: string | null, opt?: CommonDaoOptions): Promise<TM | null>
  async getByIdAsTM(id?: string | null, opt: CommonDaoOptions = {}): Promise<TM | null> {
    if (!id) return null
    const op = `getByIdAsTM(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let [dbm] = await this.cfg.db.getByIds<DBM>(table, [id])
    if (dbm && !opt.raw && this.cfg.hooks!.afterLoad) {
      dbm = (await this.cfg.hooks!.afterLoad(dbm)) || undefined
    }

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
    if (!ids.length) return []
    const op = `getByIds ${ids.length} id(s) (${_truncate(ids.slice(0, 10).join(', '), 50)})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let dbms = await (opt.tx || this.cfg.db).getByIds<DBM>(table, ids)
    if (!opt.raw && this.cfg.hooks!.afterLoad && dbms.length) {
      dbms = (await pMap(dbms, async dbm => await this.cfg.hooks!.afterLoad!(dbm))).filter(
        _isTruthy,
      )
    }

    const bms = opt.raw ? (dbms as any) : await this.dbmsToBM(dbms, opt)
    this.logResult(started, op, bms, table)
    return bms
  }

  async getByIdsAsDBM(ids: string[], opt: CommonDaoOptions = {}): Promise<Saved<DBM>[]> {
    if (!ids.length) return []
    const op = `getByIdsAsDBM ${ids.length} id(s) (${_truncate(ids.slice(0, 10).join(', '), 50)})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    let dbms = await this.cfg.db.getByIds<DBM>(table, ids)
    if (!opt.raw && this.cfg.hooks!.afterLoad && dbms.length) {
      dbms = (await pMap(dbms, async dbm => await this.cfg.hooks!.afterLoad!(dbm))).filter(
        _isTruthy,
      )
    }

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

  async requireByIdAsDBM(id: string, opt: CommonDaoOptions = {}): Promise<Saved<DBM>> {
    const r = await this.getByIdAsDBM(id, opt)
    if (!r) {
      this.throwRequiredError(id, opt)
    }
    return r
  }

  private throwRequiredError(id: string, opt: CommonDaoOptions): never {
    const table = opt.table || this.cfg.table
    throw new AppError(`DB row required, but not found in ${table}`, {
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
        table: this.cfg.table,
      })
    }
  }

  /**
   * Throws if readOnly is true
   */
  private requireObjectMutability(opt: CommonDaoOptions): void {
    if (this.cfg.immutable && !opt.allowMutability) {
      throw new AppError(DBLibError.OBJECT_IS_IMMUTABLE, {
        table: this.cfg.table,
      })
    }
  }

  private async ensureUniqueId(table: string, dbm: Saved<DBM>): Promise<void> {
    // todo: retry N times
    const existing = await this.cfg.db.getByIds<DBM>(table, [dbm.id])
    if (existing.length) {
      throw new AppError(DBLibError.NON_UNIQUE_ID, {
        table,
        ids: existing.map(i => i.id),
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
    return rows.map((r: any) => r[col])
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
    let { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    const partialQuery = !!q._selectedFieldNames
    if (!opt.raw && this.cfg.hooks!.afterLoad && rows.length) {
      rows = (await pMap(rows, async dbm => await this.cfg.hooks!.afterLoad!(dbm))).filter(
        _isTruthy,
      )
    }

    const bms = partialQuery || opt.raw ? (rows as any[]) : await this.dbmsToBM(rows, opt)
    this.logResult(started, op, bms, q.table)
    return {
      rows: bms,
      ...queryResult,
    }
  }

  async runQueryAsDBM(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<Saved<DBM>[]> {
    const { rows } = await this.runQueryExtendedAsDBM(q, opt)
    return rows
  }

  async runQueryExtendedAsDBM(
    q: DBQuery<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<RunQueryResult<Saved<DBM>>> {
    q.table = opt.table || q.table
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    let { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    if (!opt.raw && this.cfg.hooks!.afterLoad && rows.length) {
      rows = (await pMap(rows, async dbm => await this.cfg.hooks!.afterLoad!(dbm))).filter(
        _isTruthy,
      )
    }

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
    let { rows, ...queryResult } = await this.cfg.db.runQuery<DBM>(q, opt)
    if (!opt.raw && this.cfg.hooks!.afterLoad && rows.length) {
      rows = (await pMap(rows, async dbm => await this.cfg.hooks!.afterLoad!(dbm))).filter(
        _isTruthy,
      )
    }

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
      transformMap<Saved<DBM>, Saved<BM>>(
        async dbm => {
          count++
          if (partialQuery || opt.raw) return dbm as any

          if (this.cfg.hooks!.afterLoad) {
            dbm = (await this.cfg.hooks!.afterLoad(dbm))!
            if (dbm === null) return SKIP
          }

          return await this.dbmToBM(dbm, opt)
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
    mapper: AsyncMapper<Saved<DBM>, void>,
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
      transformMap<any, DBM>(
        async dbm => {
          count++
          if (partialQuery || opt.raw) return dbm

          if (this.cfg.hooks!.afterLoad) {
            dbm = (await this.cfg.hooks!.afterLoad(dbm))!
            if (dbm === null) return SKIP
          }

          return this.anyToDBM(dbm, opt)
        },
        {
          errorMode: opt.errorMode,
        },
      ),
      transformMap<Saved<DBM>, void>(mapper, {
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
  streamQueryAsDBM(
    q: DBQuery<DBM>,
    opt: CommonDaoStreamOptions<DBM> = {},
  ): ReadableTyped<Saved<DBM>> {
    q.table = opt.table || q.table
    opt.skipValidation = opt.skipValidation !== false // default true
    opt.skipConversion = opt.skipConversion !== false // default true
    opt.errorMode ||= ErrorMode.SUPPRESS

    const partialQuery = !!q._selectedFieldNames

    const stream = this.cfg.db.streamQuery<DBM>(q, opt)
    if (partialQuery || opt.raw) return stream

    return stream
      .on('error', err => stream.emit('error', err))
      .pipe(
        transformMap<any, Saved<DBM>>(
          async dbm => {
            if (this.cfg.hooks!.afterLoad) {
              dbm = (await this.cfg.hooks!.afterLoad(dbm))!
              if (dbm === null) return SKIP
            }

            return this.anyToDBM(dbm, opt)
          },
          {
            errorMode: opt.errorMode,
          },
        ),
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
  streamQuery(
    q: DBQuery<DBM>,
    opt: CommonDaoStreamOptions<Saved<BM>> = {},
  ): ReadableTyped<Saved<BM>> {
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
        .on('error', err => stream.emit('error', err))
        .pipe(
          transformMap<Saved<DBM>, Saved<BM>>(
            async dbm => {
              if (this.cfg.hooks!.afterLoad) {
                dbm = (await this.cfg.hooks!.afterLoad(dbm))!
                if (dbm === null) return SKIP
              }

              return await this.dbmToBM(dbm, opt)
            },
            {
              errorMode: opt.errorMode,
            },
          ),
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

  streamQueryIds(q: DBQuery<DBM>, opt: CommonDaoStreamOptions<string> = {}): ReadableTyped<string> {
    q.table = opt.table || q.table
    opt.errorMode ||= ErrorMode.SUPPRESS

    const stream: ReadableTyped<string> = this.cfg.db
      .streamQuery<DBM>(q.select(['id']), opt)
      .on('error', err => stream.emit('error', err))
      .pipe(
        transformMapSimple<Saved<DBM>, string>(r => r.id, {
          errorMode: ErrorMode.SUPPRESS, // cause .pipe() cannot propagate errors
        }),
      )

    return stream
  }

  async streamQueryIdsForEach(
    q: DBQuery<DBM>,
    mapper: AsyncMapper<string, void>,
    opt: CommonDaoStreamForEachOptions<string> = {},
  ): Promise<void> {
    q.table = opt.table || q.table
    opt.errorMode ||= ErrorMode.SUPPRESS

    const op = `streamQueryIdsForEach(${q.pretty()})`
    const started = this.logStarted(op, q.table, true)
    let count = 0

    await _pipeline([
      this.cfg.db.streamQuery<DBM>(q.select(['id']), opt),
      transformMapSimple<Saved<DBM>, string>(r => {
        count++
        return r.id
      }),
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
  assignIdCreatedUpdated<T extends BaseDBEntity>(obj: T, opt: CommonDaoOptions = {}): Saved<T> {
    const now = Math.floor(Date.now() / 1000)

    if (this.cfg.useCreatedProperty) {
      obj.created ||= obj.updated || now
    }

    if (this.cfg.useUpdatedProperty) {
      obj.updated = opt.preserveUpdatedCreated && obj.updated ? obj.updated : now
    }

    if (this.cfg.createId) {
      obj.id ||= this.cfg.hooks!.createNaturalId?.(obj as any) || this.cfg.hooks!.createRandomId!()
    }

    return obj as Saved<T>
  }

  // SAVE
  /**
   * Mutates with id, created, updated
   */
  async save(bm: BM, opt: CommonDaoSaveOptions<BM, DBM> = {}): Promise<Saved<BM>> {
    this.requireWriteAccess()

    if (opt.skipIfEquals && _deepJsonEquals(bm, opt.skipIfEquals)) {
      // Skipping the save operation
      return bm as Saved<BM>
    }

    const idWasGenerated = !bm.id && this.cfg.createId
    this.assignIdCreatedUpdated(bm, opt) // mutates
    let dbm = await this.bmToDBM(bm, opt)

    if (this.cfg.hooks!.beforeSave) {
      dbm = (await this.cfg.hooks!.beforeSave(dbm))!
      if (dbm === null) return bm as any
    }

    const table = opt.table || this.cfg.table
    if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, dbm)
    if (this.cfg.immutable && !opt.allowMutability && !opt.saveMethod) {
      opt = { ...opt, saveMethod: 'insert' }
    }
    const op = `save(${dbm.id})`
    const started = this.logSaveStarted(op, bm, table)
    const { excludeFromIndexes } = this.cfg
    const assignGeneratedIds = opt.assignGeneratedIds || this.cfg.assignGeneratedIds

    await this.cfg.db.saveBatch(table, [dbm], {
      excludeFromIndexes,
      assignGeneratedIds,
      ...opt,
    })

    if (assignGeneratedIds) {
      bm.id = dbm.id
    }

    this.logSaveResult(started, op, table)
    return bm as Saved<BM>
  }

  /**
   * 1. Applies the patch
   * 2. If object is the same after patching - skips saving it
   * 3. Otherwise - saves the patched object and returns it
   *
   * Similar to `save` with skipIfEquals.
   * Similar to `patch`, but doesn't load the object from the Database.
   */
  async savePatch(
    bm: Saved<BM>,
    patch: Partial<BM>,
    opt: CommonDaoSaveBatchOptions<DBM>,
  ): Promise<Saved<BM>> {
    const patched: Saved<BM> = {
      ...bm,
      ...patch,
    }

    if (_deepJsonEquals(bm, patched)) {
      // Skipping the save operation, as data is the same
      return bm
    }

    // Actually apply the patch by mutating the original object (by design)
    Object.assign(bm, patch)

    return await this.save(bm, opt)
  }

  /**
   * Convenience method to replace 3 operations (loading+patching+saving) with one:
   *
   * 1. Loads the row by id.
   * 1.1 Creates the row (via this.create()) if it doesn't exist
   * (this will cause a validation error if Patch has not enough data for the row to be valid).
   * 2. Applies the patch on top of loaded data.
   * 3. Saves (as fast as possible since the read) with the Patch applied, but only if the data has changed.
   */
  async patchById(
    id: string,
    patch: Partial<BM>,
    opt: CommonDaoSaveBatchOptions<DBM> = {},
  ): Promise<Saved<BM>> {
    let patched: Saved<BM>
    const loaded = await this.getById(id, opt)

    if (loaded) {
      patched = { ...loaded, ...patch }

      if (_deepJsonEquals(loaded, patched)) {
        // Skipping the save operation, as data is the same
        return patched
      }
    } else {
      patched = this.create({ ...patch, id }, opt)
    }

    return await this.save(patched, opt)
  }

  /**
   * Same as patchById, but takes the whole object as input.
   * This "whole object" is mutated with the patch and returned.
   * Otherwise, similar behavior as patchById.
   * It still loads the row from the DB.
   */
  async patch(
    bm: Saved<BM>,
    patch: Partial<BM>,
    opt: CommonDaoSaveBatchOptions<DBM> = {},
  ): Promise<Saved<BM>> {
    _assert(bm.id, 'patch argument object should have an id', {
      bm,
    })

    const loaded = await this.getById(bm.id, opt)

    if (loaded) {
      Object.assign(loaded, patch)

      if (_deepJsonEquals(loaded, bm)) {
        // Skipping the save operation, as data is the same
        return bm
      }

      // Make `bm` exactly the same as `loaded`
      _objectAssignExact(bm, loaded)
    } else {
      Object.assign(bm, patch)
    }

    return await this.save(bm, opt)
  }

  async saveAsDBM(dbm: DBM, opt: CommonDaoSaveBatchOptions<DBM> = {}): Promise<Saved<DBM>> {
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table

    // assigning id in case it misses the id
    // will override/set `updated` field, unless opts.preserveUpdated is set
    let row = dbm as Saved<DBM>
    if (!opt.raw) {
      const idWasGenerated = !dbm.id && this.cfg.createId
      this.assignIdCreatedUpdated(dbm, opt) // mutates
      row = this.anyToDBM(dbm, opt)
      if (opt.ensureUniqueId && idWasGenerated) await this.ensureUniqueId(table, row)
    }
    if (this.cfg.immutable && !opt.allowMutability && !opt.saveMethod) {
      opt = { ...opt, saveMethod: 'insert' }
    }
    const op = `saveAsDBM(${row.id})`
    const started = this.logSaveStarted(op, row, table)
    const { excludeFromIndexes } = this.cfg
    const assignGeneratedIds = opt.assignGeneratedIds || this.cfg.assignGeneratedIds

    if (this.cfg.hooks!.beforeSave) {
      row = (await this.cfg.hooks!.beforeSave(row))!
      if (row === null) return dbm as Saved<DBM>
    }

    await this.cfg.db.saveBatch(table, [row], {
      excludeFromIndexes,
      assignGeneratedIds,
      ...opt,
    })

    if (assignGeneratedIds) {
      dbm.id = row.id
    }

    this.logSaveResult(started, op, table)
    return row
  }

  async saveBatch(bms: BM[], opt: CommonDaoSaveBatchOptions<DBM> = {}): Promise<Saved<BM>[]> {
    if (!bms.length) return []
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table
    bms.forEach(bm => this.assignIdCreatedUpdated(bm, opt))
    let dbms = await this.bmsToDBM(bms, opt)

    if (this.cfg.hooks!.beforeSave && dbms.length) {
      dbms = (await pMap(dbms, async dbm => await this.cfg.hooks!.beforeSave!(dbm))).filter(
        _isTruthy,
      )
    }

    if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
    if (this.cfg.immutable && !opt.allowMutability && !opt.saveMethod) {
      opt = { ...opt, saveMethod: 'insert' }
    }

    const op = `saveBatch ${dbms.length} row(s) (${_truncate(
      dbms
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
    const started = this.logSaveStarted(op, bms, table)
    const { excludeFromIndexes } = this.cfg
    const assignGeneratedIds = opt.assignGeneratedIds || this.cfg.assignGeneratedIds

    await (opt.tx || this.cfg.db).saveBatch(table, dbms, {
      excludeFromIndexes,
      assignGeneratedIds,
      ...opt,
    })

    if (assignGeneratedIds) {
      dbms.forEach((dbm, i) => (bms[i]!.id = dbm.id))
    }

    this.logSaveResult(started, op, table)

    return bms as Saved<BM>[]
  }

  async saveBatchAsDBM(
    dbms: DBM[],
    opt: CommonDaoSaveBatchOptions<DBM> = {},
  ): Promise<Saved<DBM>[]> {
    if (!dbms.length) return []
    this.requireWriteAccess()
    const table = opt.table || this.cfg.table
    let rows = dbms as Saved<DBM>[]
    if (!opt.raw) {
      dbms.forEach(dbm => this.assignIdCreatedUpdated(dbm, opt)) // mutates
      rows = this.anyToDBMs(dbms as Saved<DBM>[], opt)
      if (opt.ensureUniqueId) throw new AppError('ensureUniqueId is not supported in saveBatch')
    }
    if (this.cfg.immutable && !opt.allowMutability && !opt.saveMethod) {
      opt = { ...opt, saveMethod: 'insert' }
    }
    const op = `saveBatchAsDBM ${rows.length} row(s) (${_truncate(
      rows
        .slice(0, 10)
        .map(bm => bm.id)
        .join(', '),
      50,
    )})`
    const started = this.logSaveStarted(op, rows, table)
    const { excludeFromIndexes } = this.cfg
    const assignGeneratedIds = opt.assignGeneratedIds || this.cfg.assignGeneratedIds

    if (this.cfg.hooks!.beforeSave && rows.length) {
      rows = (await pMap(rows, async row => await this.cfg.hooks!.beforeSave!(row))).filter(
        _isTruthy,
      )
    }

    await this.cfg.db.saveBatch(table, rows, {
      excludeFromIndexes,
      assignGeneratedIds,
      ...opt,
    })

    if (assignGeneratedIds) {
      rows.forEach((row, i) => (dbms[i]!.id = row.id))
    }

    this.logSaveResult(started, op, table)
    return rows
  }

  /**
   * "Streaming" is implemented by buffering incoming rows into **batches**
   * (of size opt.batchSize, which defaults to 500),
   * and then executing db.saveBatch(batch) with the concurrency
   * of opt.batchConcurrency (which defaults to 16).
   */
  streamSaveTransform(opt: CommonDaoStreamSaveOptions<DBM> = {}): Transform[] {
    this.requireWriteAccess()

    const table = opt.table || this.cfg.table
    opt.skipValidation ??= true
    opt.skipConversion ??= true
    opt.errorMode ||= ErrorMode.SUPPRESS

    if (this.cfg.immutable && !opt.allowMutability && !opt.saveMethod) {
      opt = { ...opt, saveMethod: 'insert' }
    }

    const excludeFromIndexes = opt.excludeFromIndexes || this.cfg.excludeFromIndexes
    const { beforeSave } = this.cfg.hooks!

    const { batchSize = 500, batchConcurrency = 16, errorMode } = opt

    return [
      transformMap<BM, Saved<DBM>>(
        async bm => {
          this.assignIdCreatedUpdated(bm, opt) // mutates

          let dbm = await this.bmToDBM(bm, opt)

          if (beforeSave) {
            dbm = (await beforeSave(dbm))!
            if (dbm === null) return SKIP
          }

          return dbm
        },
        {
          errorMode,
        },
      ),
      transformBuffer<DBM>({ batchSize }),
      transformMap<DBM[], DBM[]>(
        async batch => {
          await this.cfg.db.saveBatch(table, batch, {
            ...opt,
            excludeFromIndexes,
          })
          return batch
        },
        {
          concurrency: batchConcurrency,
          errorMode,
          flattenArrayOutput: true,
        },
      ),
      transformLogProgress({
        metric: 'saved',
        ...opt,
      }),
      // just to satisfy and simplify typings
      // It's easier to return Transform[], rather than (Transform | Writable)[]
      writableVoid() as Transform,
    ]
  }

  // DELETE
  /**
   * @returns number of deleted items
   */
  async deleteById(id: undefined | null, opt?: CommonDaoOptions): Promise<0>
  async deleteById(id?: string | null, opt?: CommonDaoOptions): Promise<number>
  async deleteById(id?: string | null, opt: CommonDaoOptions = {}): Promise<number> {
    if (!id) return 0
    this.requireWriteAccess()
    this.requireObjectMutability(opt)
    const op = `deleteById(${id})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const count = await this.cfg.db.deleteByIds(table, [id], opt)
    this.logSaveResult(started, op, table)
    return count
  }

  async deleteByIds(ids: string[], opt: CommonDaoOptions = {}): Promise<number> {
    if (!ids.length) return 0
    this.requireWriteAccess()
    this.requireObjectMutability(opt)
    const op = `deleteByIds(${ids.join(', ')})`
    const table = opt.table || this.cfg.table
    const started = this.logStarted(op, table)
    const count = await (opt.tx || this.cfg.db).deleteByIds(table, ids, opt)
    this.logSaveResult(started, op, table)
    return count
  }

  /**
   * Pass `stream: true` option to use Streaming: it will Stream the query, batch by 500, and execute
   * `deleteByIds` for each batch concurrently (infinite concurrency).
   * This is expected to be more memory-efficient way of deleting big numbers of rows.
   */
  async deleteByQuery(
    q: DBQuery<DBM>,
    opt: CommonDaoStreamDeleteOptions<DBM> = {},
  ): Promise<number> {
    this.requireWriteAccess()
    this.requireObjectMutability(opt)
    q.table = opt.table || q.table
    const op = `deleteByQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    let deleted = 0

    if (opt.batchSize) {
      const { batchSize, batchConcurrency = 16 } = opt

      await _pipeline([
        this.cfg.db.streamQuery<DBM>(q.select(['id']), opt),
        transformMapSimple<ObjectWithId, string>(r => r.id, {
          errorMode: ErrorMode.SUPPRESS,
        }),
        transformBuffer<string>({ batchSize }),
        transformMap<string[], void>(
          async ids => {
            deleted += await this.cfg.db.deleteByQuery(
              DBQuery.create(q.table).filterIn('id', ids),
              opt,
            )
          },
          {
            predicate: _passthroughPredicate,
            concurrency: batchConcurrency,
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

  async updateById(id: string, patch: DBPatch<DBM>, opt: CommonDaoOptions = {}): Promise<number> {
    return await this.updateByQuery(this.query().filterEq('id', id), patch, opt)
  }

  async updateByIds(
    ids: string[],
    patch: DBPatch<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<number> {
    if (!ids.length) return 0
    return await this.updateByQuery(this.query().filterIn('id', ids), patch, opt)
  }

  async updateByQuery(
    q: DBQuery<DBM>,
    patch: DBPatch<DBM>,
    opt: CommonDaoOptions = {},
  ): Promise<number> {
    this.requireWriteAccess()
    this.requireObjectMutability(opt)
    q.table = opt.table || q.table
    const op = `updateByQuery(${q.pretty()})`
    const started = this.logStarted(op, q.table)
    const updated = await this.cfg.db.updateByQuery(q, patch, opt)
    this.logSaveResult(started, op, q.table)
    return updated
  }

  // CONVERSIONS

  async dbmToBM(_dbm: undefined, opt?: CommonDaoOptions): Promise<undefined>
  async dbmToBM(_dbm?: Saved<DBM>, opt?: CommonDaoOptions): Promise<Saved<BM>>
  async dbmToBM(_dbm?: Saved<DBM>, opt: CommonDaoOptions = {}): Promise<Saved<BM> | undefined> {
    if (!_dbm) return

    // optimization: no need to run full joi DBM validation, cause BM validation will be run
    // const dbm = this.anyToDBM(_dbm, opt)
    let dbm: Saved<DBM> = { ..._dbm, ...this.cfg.hooks!.parseNaturalId!(_dbm.id) }

    if (opt.anonymize) {
      dbm = this.cfg.hooks!.anonymize!(dbm)
    }

    // DBM > BM
    const bm = await this.cfg.hooks!.beforeDBMToBM!(dbm)

    // Validate/convert BM
    // eslint-disable-next-line @typescript-eslint/return-await
    return this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)
  }

  async dbmsToBM(dbms: Saved<DBM>[], opt: CommonDaoOptions = {}): Promise<Saved<BM>[]> {
    return await pMap(dbms, async dbm => await this.dbmToBM(dbm, opt))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  async bmToDBM(bm: undefined, opt?: CommonDaoOptions): Promise<undefined>
  async bmToDBM(bm?: BM, opt?: CommonDaoOptions): Promise<Saved<DBM>>
  async bmToDBM(bm?: BM, opt?: CommonDaoOptions): Promise<Saved<DBM> | undefined> {
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

  async bmsToDBM(bms: BM[], opt: CommonDaoOptions = {}): Promise<Saved<DBM>[]> {
    // try/catch?
    return await pMap(bms, async bm => await this.bmToDBM(bm, opt))
  }

  anyToDBM(dbm: undefined, opt?: CommonDaoOptions): undefined
  anyToDBM(dbm?: any, opt?: CommonDaoOptions): Saved<DBM>
  anyToDBM(dbm?: Saved<DBM>, opt: CommonDaoOptions = {}): Saved<DBM> | undefined {
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

  anyToDBMs(entities: Saved<DBM>[], opt: CommonDaoOptions = {}): Saved<DBM>[] {
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
    const tm = this.cfg.hooks!.beforeBMToTM!(bm)

    // Validate/convert DBM
    return this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)
  }

  bmsToTM(bms: Saved<BM>[], opt: CommonDaoOptions = {}): TM[] {
    // try/catch?
    return bms.map(bm => this.bmToTM(bm, opt))
  }

  /**
   * Returns *converted value*.
   * Validates (unless `skipValidation=true` passed).
   *
   * Does NOT mutate the object.
   */
  validateAndConvert<IN, OUT = IN>(
    obj: Partial<IN>,
    schema: ObjectSchema<IN> | AjvSchema<IN> | ZodSchema<IN> | undefined,
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

    let error: JoiValidationError | AjvValidationError | ZodValidationError<IN> | undefined
    let convertedValue: any

    if (schema instanceof ZodSchema) {
      // Zod schema
      const vr = zSafeValidate(obj as IN, schema)
      error = vr.error
      convertedValue = vr.data
    } else if (schema instanceof AjvSchema) {
      // Ajv schema
      convertedValue = obj // because Ajv mutates original object

      error = schema.getValidationError(obj as IN, {
        objectName,
      })
    } else {
      // Joi
      const vr = getValidationResult(obj, schema, objectName)
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
    await this.cfg.db.createTable(this.cfg.table, schema, opt)
  }

  /**
   * Proxy to this.cfg.db.ping
   */
  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async runInTransaction(
    fn: CommonDaoTransactionFn,
    opt?: CommonDBTransactionOptions,
  ): Promise<void> {
    await this.cfg.db.runInTransaction(async tx => {
      const daoTx = new CommonDaoTransaction(tx, this.cfg.logger!)

      try {
        await fn(daoTx)
      } catch (err) {
        await daoTx.rollback() // graceful rollback that "never throws"
        throw err
      }
    }, opt)
  }

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

  protected logStarted(op: string, table: string, force = false): UnixTimestampMillisNumber {
    if (this.cfg.logStarted || force) {
      this.cfg.logger?.log(`>> ${table}.${op}`)
    }
    return Date.now()
  }

  protected logSaveStarted(op: string, items: any, table: string): UnixTimestampMillisNumber {
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

/**
 * Transaction is committed when the function returns resolved Promise (aka "returns normally").
 *
 * Transaction is rolled back when the function returns rejected Promise (aka "throws").
 */
export type CommonDaoTransactionFn = (tx: CommonDaoTransaction) => Promise<void>

/**
 * Transaction context.
 * Has similar API than CommonDao, but all operations are performed in the context of the transaction.
 */
export class CommonDaoTransaction {
  constructor(
    private tx: DBTransaction,
    private logger: CommonLogger,
  ) {}

  /**
   * Perform a graceful rollback without throwing/re-throwing any error.
   */
  async rollback(): Promise<void> {
    try {
      await this.tx.rollback()
    } catch (err) {
      // graceful rollback without re-throw
      this.logger.error(err)
    }
  }

  async getById<BM extends PartialObjectWithId, DBM extends PartialObjectWithId>(
    dao: CommonDao<BM, DBM, any>,
    id?: string | null,
    opt?: CommonDaoOptions,
  ): Promise<Saved<BM> | null> {
    if (!id) return null
    return (await this.getByIds(dao, [id], opt))[0] || null
  }

  async getByIds<BM extends PartialObjectWithId, DBM extends PartialObjectWithId>(
    dao: CommonDao<BM, DBM, any>,
    ids: string[],
    opt?: CommonDaoOptions,
  ): Promise<Saved<BM>[]> {
    return await dao.getByIds(ids, { ...opt, tx: this.tx })
  }

  // todo: Queries inside Transaction are not supported yet
  // async runQuery<BM extends PartialObjectWithId, DBM extends ObjectWithId>(
  //   dao: CommonDao<BM, DBM, any>,
  //   q: DBQuery<DBM>,
  //   opt?: CommonDaoOptions,
  // ): Promise<Saved<BM>[]> {
  //   try {
  //     return await dao.runQuery(q, { ...opt, tx: this.tx })
  //   } catch (err) {
  //     await this.rollback()
  //     throw err
  //   }
  // }

  async save<BM extends PartialObjectWithId, DBM extends PartialObjectWithId>(
    dao: CommonDao<BM, DBM, any>,
    bm: BM,
    opt?: CommonDaoSaveBatchOptions<DBM>,
  ): Promise<Saved<BM>> {
    return (await this.saveBatch(dao, [bm], opt))[0]!
  }

  async saveBatch<BM extends PartialObjectWithId, DBM extends PartialObjectWithId>(
    dao: CommonDao<BM, DBM, any>,
    bms: BM[],
    opt?: CommonDaoSaveBatchOptions<DBM>,
  ): Promise<Saved<BM>[]> {
    return await dao.saveBatch(bms, { ...opt, tx: this.tx })
  }

  async deleteById(
    dao: CommonDao<any>,
    id?: string | null,
    opt?: CommonDaoOptions,
  ): Promise<number> {
    if (!id) return 0
    return await this.deleteByIds(dao, [id], opt)
  }

  async deleteByIds(dao: CommonDao<any>, ids: string[], opt?: CommonDaoOptions): Promise<number> {
    return await dao.deleteByIds(ids, { ...opt, tx: this.tx })
  }
}
