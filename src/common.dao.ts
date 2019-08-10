import {
  Debug,
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
} from '@naturalcycles/nodejs-lib'
import { since } from '@naturalcycles/time-lib'
import { Observable } from 'rxjs'
import { count, map, mergeMap } from 'rxjs/operators'
import {
  BaseDBEntity,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDB,
  DBModelType,
  ObjectWithId,
} from './db.model'
import { DBQuery } from './dbQuery'
import { assignIdCreatedUpdated, createdUpdatedFields } from './model.util'

export enum CommonDaoLogLevel {
  /**
   * Same as undefined
   */
  NONE = 0,
  OPERATIONS = 10,
  DATA_SINGLE = 20,
  DATA_FULL = 30,
}

export interface CommonDaoCfg<BM, DBM, DB extends CommonDB = CommonDB> {
  db: DB
  table: string
  dbmSchema?: ObjectSchemaTyped<DBM>
  bmSchema?: ObjectSchemaTyped<BM>

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

const log = Debug('db-lib:commondao')

/**
 * Lowest common denominator API between supported Databases.
 *
 * DBM = Database model (how it's stored in DB)
 * BM = Backend model (optimized for API access)
 */
export class CommonDao<BM extends BaseDBEntity = any, DBM extends BaseDBEntity = BM> {
  constructor (protected cfg: CommonDaoCfg<BM, DBM>) {
    this.cfg = {
      logLevel: CommonDaoLogLevel.OPERATIONS,
      ...cfg,
    }
  }

  protected logResult (started: number, op: string, res: any): void {
    if (!this.cfg.logLevel) return

    let logRes: any
    const args: any[] = []

    if (Array.isArray(res)) {
      logRes = `${res.length} row(s)`
      if (res.length && this.cfg.logLevel >= CommonDaoLogLevel.DATA_FULL) {
        args.push(res.slice(0, 10)) // max 10 items
      }
    } else if (res) {
      logRes = `1 row`
      if (this.cfg.logLevel >= CommonDaoLogLevel.DATA_SINGLE) {
        args.push(res)
      }
    } else {
      logRes = `undefined`
    }

    log(...[`<< ${this.cfg.table}.${op}: ${logRes} in ${since(started)}`].concat(args))
  }

  protected logSaveResult (started: number, op: string): void {
    if (!this.cfg.logLevel) return
    log(`<< ${this.cfg.table}.${op} in ${since(started)}`)
  }

  protected logStarted (op: string, force = false): number {
    if (this.cfg.logStarted || force) {
      log(`>> ${this.cfg.table}.${op}`)
    }
    return Date.now()
  }

  protected logSaveStarted (op: string, items: any): number {
    if (this.cfg.logStarted) {
      const args: any[] = [`>> ${this.cfg.table}.${op}`]
      if (Array.isArray(items)) {
        if (items.length && this.cfg.logLevel! >= CommonDaoLogLevel.DATA_FULL) {
          args.push(...items.slice(0, 10))
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

  // to be extended
  beforeCreate (bm: Partial<BM>): BM {
    return bm as BM
  }

  // to be extended
  beforeDBMValidate (dbm: DBM): DBM {
    return dbm
  }

  // to be extended
  async beforeDBMToBM (dbm: DBM): Promise<BM> {
    return dbm as any
  }

  // to be extended
  async beforeBMToDBM (bm: BM): Promise<DBM> {
    return bm as any
  }

  // CREATE
  create (input: BM, opts: CommonDaoOptions = {}): BM {
    if (opts.throwOnError === undefined) {
      opts.throwOnError = this.cfg.throwOnDaoCreateObject
    }

    let bm = Object.assign({}, this.beforeCreate(input))
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opts)

    // If no SCHEMA - return as is
    return Object.assign({}, bm)
  }

  // GET
  async getById (id?: string, opts?: CommonDaoOptions): Promise<BM | undefined> {
    if (!id) return
    const op = `getById(${id})`
    const started = this.logStarted(op)
    const [dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const bm = await this.dbmToBM(dbm, opts)
    this.logResult(started, op, bm)
    return bm
  }

  async getByIdOrCreate (id: string, bmToCreate: BM, opts?: CommonDaoOptions): Promise<BM> {
    const bm = await this.getById(id, opts)
    if (bm) return bm

    return this.create(bmToCreate, opts)
  }

  async getByIdOrEmpty (id: string, opts?: CommonDaoOptions): Promise<BM> {
    const bm = await this.getById(id, opts)
    if (bm) return bm

    return this.create({ id, ...createdUpdatedFields() } as any, opts)
  }

  async getByIdAsDBM (id?: string, opts?: CommonDaoOptions): Promise<DBM | undefined> {
    if (!id) return
    const op = `getByIdAsDBM(${id})`
    const started = this.logStarted(op)
    const [entity] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const dbm = this.anyToDBM(entity, opts)
    this.logResult(started, op, dbm)
    return dbm
  }

  async getByIds (ids: string[], opts?: CommonDaoOptions): Promise<BM[]> {
    const op = `getByIds(${ids.join(', ')})`
    const started = this.logStarted(op)
    const dbms = await this.cfg.db.getByIds(this.cfg.table, ids)
    const bms = await this.dbmsToBM(dbms, opts)
    this.logResult(started, op, bms)
    return bms
  }

  async requireById (id: string, opts?: CommonDaoOptions): Promise<BM> {
    const r = await this.getById(id, opts)
    if (!r) throw new Error(`DB record required, but not found: ${this.cfg.table}.${id}`)
    return r
  }

  async getBy (by: string, value: any, limit = 0, opts?: CommonDaoOptions): Promise<BM[]> {
    const q = this.createQuery()
      .filter(by, '=', value)
      .limit(limit)
    return this.runQuery(q, opts)
  }

  async getOneBy (by: string, value: any, opts?: CommonDaoOptions): Promise<BM | undefined> {
    const q = this.createQuery()
      .filter(by, '=', value)
      .limit(1)
    const [bm] = await this.runQuery(q, opts)
    return bm
  }

  // QUERY
  createQuery (): DBQuery<DBM> {
    return new DBQuery<DBM>(this.cfg.table)
  }

  async runQuery (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<BM[]> {
    const op = `runQuery(${q.pretty()})`
    const started = this.logStarted(op)
    const dbms = await this.cfg.db.runQuery(q, opts)
    const bms = await this.dbmsToBM(dbms, opts)
    this.logResult(started, op, bms)
    return bms
  }

  async runQueryAsDBM (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<DBM[]> {
    const op = `runQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op)
    const entities = await this.cfg.db.runQuery(q, opts)
    const dbms = this.anyToDBMs(entities, opts)
    this.logResult(started, op, dbms)
    return dbms
  }

  async runQueryCount (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<number> {
    const op = `runQueryCount(${q.pretty()})`
    const started = this.logStarted(op)
    const count = await this.cfg.db.runQueryCount(q, opts)
    if (this.cfg.logLevel! >= CommonDaoLogLevel.OPERATIONS) {
      log(`<< ${this.cfg.table}.${op}: ${count} rows in ${since(started)}`)
    }
    return count
  }

  streamQuery (q: DBQuery<DBM>, opts?: CommonDaoOptions): Observable<BM> {
    const op = `streamQuery(${q.pretty()})`
    const started = this.logStarted(op, true)
    const obs = this.cfg.db.streamQuery(q).pipe(mergeMap(dbm => this.dbmToBM(dbm, opts)))
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

  streamQueryAsDBM (q: DBQuery<DBM>, opts?: CommonDaoOptions): Observable<DBM> {
    const op = `streamQueryAsDBM(${q.pretty()})`
    const started = this.logStarted(op, true)
    const obs = this.cfg.db.streamQuery(q).pipe(map(entity => this.anyToDBM(entity, opts)))
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

  async queryIds (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<string[]> {
    const rows = await this.cfg.db.runQuery<ObjectWithId>(q.select(['id']), opts)
    return rows.map(row => row.id)
  }

  streamQueryIds (q: DBQuery<DBM>, opts?: CommonDaoOptions): Observable<string> {
    return this.cfg.db.streamQuery<ObjectWithId>(q.select(['id']), opts).pipe(map(row => row.id))
  }

  // SAVE
  async save (bm: BM, opts?: CommonDaoSaveOptions): Promise<BM> {
    const op = `save(${bm.id})`
    const started = this.logSaveStarted(op, bm)
    const dbm = await this.bmToDBM(bm, opts)
    const [savedDBM] = await this.cfg.db.saveBatch(this.cfg.table, [dbm], opts)
    const savedBM = await this.dbmToBM(savedDBM, opts)
    this.logSaveResult(started, op)
    return savedBM
  }

  async saveAsDBM (dbm: DBM, opts?: CommonDaoSaveOptions): Promise<DBM> {
    const op = `saveAsDBM(${dbm.id})`
    const started = this.logSaveStarted(op, dbm)
    const [savedDBM] = await this.cfg.db.saveBatch(this.cfg.table, [dbm], opts)
    this.logSaveResult(started, op)
    return savedDBM
  }

  async saveBatch (bms: BM[], opts?: CommonDaoSaveOptions): Promise<BM[]> {
    const op = `saveBatch(${bms.map(bm => bm.id).join(', ')})`
    const started = this.logSaveStarted(op, bms)
    const dbms = await this.bmsToDBM(bms, opts)
    const savedDBMs = await this.cfg.db.saveBatch(this.cfg.table, dbms, opts)
    const savedBMs = await this.dbmsToBM(savedDBMs, opts)
    this.logSaveResult(started, op)
    return savedBMs
  }

  async saveBatchAsDBM (_dbms: DBM[], opts?: CommonDaoSaveOptions): Promise<DBM[]> {
    const op = `saveBatch(${_dbms.map(dbm => dbm.id).join(', ')})`
    const started = this.logSaveStarted(op, _dbms)
    const dbms = this.anyToDBMs(_dbms, opts)
    const savedResults = await this.cfg.db.saveBatch(this.cfg.table, dbms, opts)
    const savedDBMs = this.anyToDBMs(savedResults, opts)
    this.logSaveResult(started, op)
    return savedDBMs
  }

  // DELETE
  /**
   * @returns array of deleted items' ids
   */
  async deleteById (id?: string): Promise<string[]> {
    if (!id) return []
    const op = `deleteById(${id})`
    const started = this.logStarted(op)
    const ids = await this.cfg.db.deleteByIds(this.cfg.table, [id])
    this.logSaveResult(started, op)
    return ids
  }

  async deleteByIds (ids: string[]): Promise<string[]> {
    const op = `deleteByIds(${ids.join(', ')})`
    const started = this.logStarted(op)
    const deletedIds = await this.cfg.db.deleteByIds(this.cfg.table, ids)
    this.logSaveResult(started, op)
    return deletedIds
  }

  async deleteBy (by: string, value: any, limit = 0, opts?: CommonDaoOptions): Promise<string[]> {
    const op = `deleteBy(${by} = ${value})`
    const started = this.logStarted(op)
    const ids = await this.cfg.db.deleteBy(this.cfg.table, by, value, limit, opts)
    this.logSaveResult(started, op)
    return ids
  }

  // CONVERSIONS

  async dbmToBM (entity?: any, opts: CommonDaoOptions = {}): Promise<BM> {
    if (!entity) return undefined as any

    const dbm = this.anyToDBM(entity, opts)

    // DBM > BM
    const bm = await this.beforeDBMToBM(dbm)

    // Validate/convert BM
    return this.validateAndConvert<BM>(bm, this.cfg.bmSchema, DBModelType.BM, opts)
  }

  async dbmsToBM (dbms: DBM[], opts: CommonDaoOptions = {}): Promise<BM[]> {
    return Promise.all(dbms.map(dbm => this.dbmToBM(dbm, opts)))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  async bmToDBM (bm: BM, opts: CommonDaoOptions = {}): Promise<DBM> {
    if (bm === undefined) return undefined as any

    // Does not mutate
    bm = assignIdCreatedUpdated(bm, opts.preserveUpdatedCreated)

    // Validate/convert BM
    // bm gets assigned to the new reference
    bm = this.validateAndConvert(bm, this.cfg.bmSchema, DBModelType.BM, opts)

    // BM > DBM
    const dbm = await this.beforeBMToDBM(bm)

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.cfg.dbmSchema, DBModelType.DBM, opts)
  }

  async bmsToDBM (bms: BM[], opts: CommonDaoOptions = {}): Promise<DBM[]> {
    // try/catch?
    return Promise.all(bms.map(bm => this.bmToDBM(bm, opts)))
  }

  anyToDBM (entity: any, opts: CommonDaoOptions = {}): DBM {
    if (!entity) return undefined as any

    // Validate/convert DBM
    return this.validateAndConvert<DBM>(entity, this.cfg.dbmSchema, DBModelType.DBM, opts)
  }

  anyToDBMs (entities: any[], opts: CommonDaoOptions = {}): DBM[] {
    return entities.map(entity => this.anyToDBM(entity, opts))
  }

  /**
   * Returns *converted value*.
   * Validates (unless `validate=false` passed).
   * Throws only if `throwOnError=true` passed OR if `env().throwOnEntityValidationError`
   */
  validateAndConvert<T = any> (
    obj: T,
    schema?: ObjectSchemaTyped<T>,
    modelType?: DBModelType,
    opts: CommonDaoOptions = {},
  ): T {
    // Pre-validation hooks
    if (modelType === DBModelType.DBM) {
      obj = this.beforeDBMValidate(obj as any) as any
    }

    // Return as is if no schema is passed
    if (!schema) return obj

    // This will Convert and Validate
    const { value, error } = getValidationResult<T>(obj, schema, this.cfg.table + (modelType || ''))

    const { skipValidation, throwOnError } = opts

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
}
