import {
  Debug,
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
  stringId,
} from '@naturalcycles/nodejs-lib'
import { since } from '@naturalcycles/time-lib'
import { Observable } from 'rxjs'
import { count, map, mergeMap } from 'rxjs/operators'
import { CommonDB } from './common.db'
import {
  BaseDBEntity,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  DBModelType,
  RunQueryResult,
  Saved,
  SavedDBEntity,
} from './db.model'
import { DBQuery, RunnableDBQuery } from './dbQuery'

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
  async beforeBMToTM(bm: BM): Promise<TM> {
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
  async getById(id?: string, opt?: CommonDaoOptions): Promise<Saved<BM> | undefined> {
    if (!id) return
    const op = `getById(${id})`
    const started = this.logStarted(op)
    const [dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const bm = await this.dbmToBM(dbm as DBM, opt)
    this.logResult(started, op, bm)
    return bm
  }

  async getByIdOrCreate(id: string, bmToCreate: BM, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create(bmToCreate, opt)
  }

  async getByIdOrEmpty(id: string, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const bm = await this.getById(id, opt)
    if (bm) return bm

    return this.create({ id } as BM, opt)
  }

  async getByIdAsDBM(id?: string, opt?: CommonDaoOptions): Promise<DBM | undefined> {
    if (!id) return
    const op = `getByIdAsDBM(${id})`
    const started = this.logStarted(op)
    const [_dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const dbm = this.anyToDBM(_dbm as DBM, opt)
    this.logResult(started, op, dbm)
    return dbm
  }

  async getByIdAsTM(id?: string, opt?: CommonDaoOptions): Promise<TM | undefined> {
    if (!id) return
    const op = `getByIdAsTM(${id})`
    const started = this.logStarted(op)
    const [dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const bm = await this.dbmToBM(dbm as DBM, opt)
    const tm = await this.bmToTM(bm as BM, opt)
    this.logResult(started, op, tm)
    return tm
  }

  async getByIds(ids: string[], opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    const op = `getByIds(${ids.join(', ')})`
    const started = this.logStarted(op)
    const dbms = await this.cfg.db.getByIds<DBM>(this.cfg.table, ids)
    const bms = await this.dbmsToBM(dbms as DBM[], opt)
    this.logResult(started, op, bms)
    return bms
  }

  async requireById(id: string, opt?: CommonDaoOptions): Promise<Saved<BM>> {
    const r = await this.getById(id, opt)
    if (!r) throw new Error(`DB record required, but not found: ${this.cfg.table}.${id}`)
    return r
  }

  async getBy(by: string, value: any, limit = 0, opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    const q = this.createQuery()
      .filter(by, '=', value)
      .limit(limit)
    return await this.runQuery(q, opt)
  }

  async getOneBy(by: string, value: any, opt?: CommonDaoOptions): Promise<Saved<BM> | undefined> {
    const q = this.createQuery()
      .filter(by, '=', value)
      .limit(1)
    const [bm] = await this.runQuery(q, opt)
    return bm
  }

  async getAll(): Promise<Saved<BM>[]> {
    return await this.runQuery(this.createQuery())
  }

  // QUERY
  createQuery(name?: string): RunnableDBQuery<BM, DBM, TM> {
    return new RunnableDBQuery(this, name)
  }

  async runQuery(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    const { records } = await this.runQueryExtended(q, opt)
    return records
  }

  async runQueryExtended(
    q: DBQuery<DBM>,
    opt?: CommonDaoOptions,
  ): Promise<RunQueryResult<Saved<BM>>> {
    const op = `runQuery(${q.pretty()})`
    const started = this.logStarted(op)
    const { records, ...queryResult } = await this.cfg.db.runQuery(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const bms = partialQuery ? (records as any[]) : await this.dbmsToBM(records as DBM[], opt)
    this.logResult(started, op, bms)
    return {
      records: bms,
      ...queryResult,
    }
  }

  async runQueryAsDBM(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<DBM[]> {
    const { records } = await this.runQueryExtendedAsDBM(q, opt)
    return records
  }

  async runQueryExtendedAsDBM(
    q: DBQuery<DBM>,
    opt?: CommonDaoOptions,
  ): Promise<RunQueryResult<DBM>> {
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op)
    const { records, ...queryResult } = await this.cfg.db.runQuery(q, opt)
    const partialQuery = !!q._selectedFieldNames
    const dbms = partialQuery ? records : this.anyToDBMs(records as DBM[], opt)
    this.logResult(started, op, dbms)
    return { records: dbms as DBM[], ...queryResult }
  }

  async runQueryCount(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<number> {
    const op = `runQueryCount(${q.pretty()})`
    const started = this.logStarted(op)
    const count = await this.cfg.db.runQueryCount(q, opt)
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${this.cfg.table}.${op}: ${count} rows in ${since(started)}`)
    }
    return count
  }

  streamQuery(q: DBQuery<DBM>, opt?: CommonDaoOptions): Observable<Saved<BM>> {
    const op = `streamQuery(${q.pretty()})`
    const started = this.logStarted(op, true)
    const partialQuery = !!q._selectedFieldNames
    const obs = this.cfg.db.streamQuery(q).pipe(
      mergeMap(async dbm => {
        if (partialQuery) return (dbm as any) as BM
        return await this.dbmToBM(dbm, opt)
      }),
    )
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      void obs
        .pipe(count())
        .toPromise()
        .then(num => {
          log(`<< ${this.cfg.table}.${op}: ${num} rows in ${since(started)}`)
        })
    }
    return obs as Observable<Saved<BM>>
  }

  streamQueryAsDBM(q: DBQuery<DBM>, opt?: CommonDaoOptions): Observable<DBM> {
    const op = `streamQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, true)
    const partialQuery = !!q._selectedFieldNames
    const obs = this.cfg.db.streamQuery(q).pipe(
      map(entity => {
        if (partialQuery) return entity
        return this.anyToDBM(entity, opt)
      }),
    )
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      void obs
        .pipe(count())
        .toPromise()
        .then(num => {
          log(`<< ${this.cfg.table}.${op}: ${num} rows in ${since(started)}`)
        })
    }
    return obs
  }

  async queryIds(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<string[]> {
    const { records } = await this.cfg.db.runQuery(q.select(['id']), opt)
    return records.map(r => r.id)
  }

  streamQueryIds(q: DBQuery<DBM>, opt?: CommonDaoOptions): Observable<string> {
    return this.cfg.db.streamQuery(q.select(['id']), opt).pipe(map(row => row.id))
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
    const dbm = await this.bmToDBM(bm, opt) // does assignIdCreatedUpdated
    const op = `save(${dbm.id})`
    const started = this.logSaveStarted(op, bm)
    await this.cfg.db.saveBatch(this.cfg.table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    const savedBM = await this.dbmToBM(dbm, opt)
    this.logSaveResult(started, op)
    return savedBM
  }

  async saveAsDBM(dbm: DBM, opt: CommonDaoSaveOptions = {}): Promise<DBM> {
    // assigning id in case it misses the id
    // will override/set `updated` field, unless opts.preserveUpdated is set
    dbm = this.assignIdCreatedUpdated(dbm, opt) as DBM
    const op = `saveAsDBM(${dbm.id})`
    const started = this.logSaveStarted(op, dbm)
    await this.cfg.db.saveBatch(this.cfg.table, [dbm], {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    this.logSaveResult(started, op)
    return dbm
  }

  async saveBatch(bms: BM[], opt: CommonDaoSaveOptions = {}): Promise<Saved<BM>[]> {
    const dbms = await this.bmsToDBM(bms, opt)
    const op = `saveBatch(${dbms.map(bm => bm.id).join(', ')})`
    const started = this.logSaveStarted(op, bms)
    await this.cfg.db.saveBatch(this.cfg.table, dbms, {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    const savedBMs = await this.dbmsToBM(dbms, opt)
    this.logSaveResult(started, op)
    return savedBMs
  }

  async saveBatchAsDBM(_dbms: DBM[], opt: CommonDaoSaveOptions = {}): Promise<DBM[]> {
    const dbms = this.anyToDBMs(_dbms, opt)
    const op = `saveBatch(${dbms.map(dbm => dbm.id).join(', ')})`
    const started = this.logSaveStarted(op, dbms)
    const dbms2 = this.anyToDBMs(dbms, opt)
    await this.cfg.db.saveBatch(this.cfg.table, dbms2, {
      excludeFromIndexes: this.cfg.excludeFromIndexes,
      ...opt,
    })
    const savedDBMs = this.anyToDBMs(dbms2, opt)
    this.logSaveResult(started, op)
    return savedDBMs
  }

  // DELETE
  /**
   * @returns number of deleted items
   */
  async deleteById(id?: string): Promise<number> {
    if (!id) return 0
    const op = `deleteById(${id})`
    const started = this.logStarted(op)
    const ids = await this.cfg.db.deleteByIds(this.cfg.table, [id])
    this.logSaveResult(started, op)
    return ids
  }

  async deleteByIds(ids: string[]): Promise<number> {
    const op = `deleteByIds(${ids.join(', ')})`
    const started = this.logStarted(op)
    const deletedIds = await this.cfg.db.deleteByIds(this.cfg.table, ids)
    this.logSaveResult(started, op)
    return deletedIds
  }

  async deleteByQuery(q: DBQuery<DBM>, opt?: CommonDaoOptions): Promise<number> {
    const op = `deleteByQuery(${q.pretty()})`
    const started = this.logStarted(op)
    const ids = await this.cfg.db.deleteByQuery(q, opt)
    this.logSaveResult(started, op)
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

  async bmToTM(bm: BM, opt?: CommonDaoOptions): Promise<TM> {
    if (bm === undefined) return undefined as any

    // Validate/convert BM
    // bm gets assigned to the new reference
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opt)

    // BM > TM
    const tm = await this.beforeBMToTM(bm)

    // Validate/convert DBM
    return this.validateAndConvert(tm, this.cfg.tmSchema, DBModelType.TM, opt)
  }

  async bmsToTM(bms: BM[], opt: CommonDaoOptions = {}): Promise<TM[]> {
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
   * Validates (unless `validate=false` passed).
   * Throws only if `throwOnError=true` passed OR if `env().throwOnEntityValidationError`
   */
  validateAndConvert<IN = any, OUT = IN>(
    obj: IN,
    schema?: ObjectSchemaTyped<IN>,
    modelType?: DBModelType,
    opt: CommonDaoOptions = {},
  ): OUT {
    // Pre-validation hooks
    if (modelType === DBModelType.DBM) {
      obj = this.beforeDBMValidate(obj as any) as any
    }

    // Return as is if no schema is passed
    if (!schema) {
      return obj as any
    }

    // This will Convert and Validate
    const { value, error } = getValidationResult<IN, OUT>(
      obj,
      schema,
      this.cfg.table + (modelType || ''),
    )

    const { skipValidation, throwOnError } = opt

    // If we care about validation and there's an error
    if (error && !skipValidation) {
      if (throwOnError || (this.cfg.throwOnEntityValidationError && throwOnError === undefined)) {
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

  protected logResult(started: number, op: string, res: any): void {
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

    log(...[`<< ${this.cfg.table}.${op}: ${logRes} in ${since(started)}`].concat(args))
  }

  protected logSaveResult(started: number, op: string): void {
    if (!this.cfg.logLevel) return
    log(`<< ${this.cfg.table}.${op} in ${since(started)}`)
  }

  protected logStarted(op: string, force = false): number {
    if (this.cfg.logStarted || force) {
      log(`>> ${this.cfg.table}.${op}`)
    }
    return Date.now()
  }

  protected logSaveStarted(op: string, items: any): number {
    if (this.cfg.logStarted) {
      const args: any[] = [`>> ${this.cfg.table}.${op}`]
      if (Array.isArray(items)) {
        if (items.length && this.cfg.logLevel! >= CommonDaoLogLevel.DATA_FULL) {
          args.push('\n', ...items.slice(0, 10))
        } else {
          args.push(`${items.length} rows`)
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
