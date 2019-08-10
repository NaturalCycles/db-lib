import {
  anyObjectSchema,
  Debug,
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
  stringId,
} from '@naturalcycles/nodejs-lib'
import { since } from '@naturalcycles/time-lib'
import { Observable } from 'rxjs'
import { count, map, mergeMap } from 'rxjs/operators'
import {
  BaseDBEntity,
  baseDBEntitySchema,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDB,
  DBModelType,
  ObjectWithId,
  Unsaved,
} from './db.model'
import { DBQuery } from './dbQuery'

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
  dbmUnsavedSchema?: ObjectSchemaTyped<Unsaved<DBM>>
  bmUnsavedSchema?: ObjectSchemaTyped<Unsaved<BM>>

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

    this.bmSavedSchema = baseDBEntitySchema.concat(cfg.bmUnsavedSchema || anyObjectSchema)
    this.dbmSavedSchema = baseDBEntitySchema.concat(cfg.dbmUnsavedSchema || anyObjectSchema)
  }

  bmSavedSchema!: ObjectSchemaTyped<BM>
  dbmSavedSchema!: ObjectSchemaTyped<DBM>

  /**
   * To be extended
   */
  createId (dbm: Unsaved<DBM>): string {
    return stringId()
  }

  /**
   * To be extended
   */
  parseNaturalId (id: string): Partial<DBM> {
    return {}
  }

  /**
   * To be extended
   */
  beforeCreate (bm: Unsaved<BM>): Unsaved<BM> {
    return bm
  }

  /**
   * To be extended
   */
  beforeDBMValidate (dbm: DBM): DBM {
    return dbm
  }

  /**
   * To be extended
   */
  async beforeDBMToBM (dbm: DBM): Promise<BM> {
    return dbm as any
  }

  /**
   * To be extended
   */
  async beforeBMToDBM (bm: Unsaved<BM>): Promise<DBM> {
    return bm as any
  }

  // CREATE
  create (input: Unsaved<BM>, opts: CommonDaoOptions = {}): Unsaved<BM> {
    if (opts.throwOnError === undefined) {
      opts.throwOnError = this.cfg.throwOnDaoCreateObject
    }

    let bm = Object.assign({}, this.beforeCreate(input))
    bm = this.validateAndConvert(bm, this.cfg.bmUnsavedSchema, DBModelType.BM, opts)

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

  async getByIdOrCreate (
    id: string,
    bmToCreate: Unsaved<BM>,
    opts?: CommonDaoOptions,
  ): Promise<Unsaved<BM>> {
    const bm = await this.getById(id, opts)
    if (bm) return bm

    return this.create(bmToCreate, opts)
  }

  async getByIdOrEmpty (id: string, opts?: CommonDaoOptions): Promise<Unsaved<BM>> {
    const bm = await this.getById(id, opts)
    if (bm) return bm

    return this.create({ id } as Unsaved<BM>, opts)
  }

  async getByIdAsDBM (id?: string, opts?: CommonDaoOptions): Promise<DBM | undefined> {
    if (!id) return
    const op = `getByIdAsDBM(${id})`
    const started = this.logStarted(op)
    const [_dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    const dbm = this.anyToDBM(_dbm, opts)
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

  assignIdCreatedUpdated (dbm: Unsaved<DBM>, opts: CommonDaoOptions = {}): DBM {
    const now = Math.floor(Date.now() / 1000)

    return {
      ...(dbm as DBM),
      id: dbm.id || this.createId(dbm),
      created: dbm.created || dbm.updated || now,
      updated: opts.preserveUpdatedCreated && dbm.updated ? dbm.updated : now,
    }
  }

  // SAVE
  async save (bm: Unsaved<BM>, opts: CommonDaoSaveOptions = {}): Promise<BM> {
    const dbm = await this.bmToDBM(bm, opts)
    const op = `save(${dbm.id})`
    const started = this.logSaveStarted(op, bm)
    const [savedDBM] = await this.cfg.db.saveBatch(this.cfg.table, [dbm], opts)
    const savedBM = await this.dbmToBM(savedDBM, opts)
    this.logSaveResult(started, op)
    return savedBM
  }

  async saveAsDBM (_dbm: Unsaved<DBM>, opts: CommonDaoSaveOptions = {}): Promise<DBM> {
    const dbm = this.anyToDBM(_dbm, opts)
    const op = `saveAsDBM(${dbm.id})`
    const started = this.logSaveStarted(op, dbm)
    const [savedDBM] = await this.cfg.db.saveBatch(this.cfg.table, [dbm], opts)
    this.logSaveResult(started, op)
    return savedDBM
  }

  async saveBatch (bms: Unsaved<BM>[], opts: CommonDaoSaveOptions = {}): Promise<BM[]> {
    const dbms = await this.bmsToDBM(bms, opts)
    const op = `saveBatch(${dbms.map(bm => bm.id).join(', ')})`
    const started = this.logSaveStarted(op, bms)
    const savedDBMs = await this.cfg.db.saveBatch(this.cfg.table, dbms, opts)
    const savedBMs = await this.dbmsToBM(savedDBMs, opts)
    this.logSaveResult(started, op)
    return savedBMs
  }

  async saveBatchAsDBM (_dbms: Unsaved<DBM>[], opts: CommonDaoSaveOptions = {}): Promise<DBM[]> {
    const dbms = this.anyToDBMs(_dbms, opts)
    const op = `saveBatch(${dbms.map(dbm => dbm.id).join(', ')})`
    const started = this.logSaveStarted(op, dbms)
    const dbms2 = this.anyToDBMs(dbms, opts)
    const savedResults = await this.cfg.db.saveBatch(this.cfg.table, dbms2, opts)
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

  async dbmToBM (_dbm: DBM, opts: CommonDaoOptions = {}): Promise<BM> {
    if (!_dbm) return undefined as any

    const dbm = this.anyToDBM(_dbm, opts)

    // DBM > BM
    const bm = await this.beforeDBMToBM(dbm)

    // Validate/convert BM
    return this.validateAndConvert<BM>(bm, this.bmSavedSchema, DBModelType.BM, opts)
  }

  async dbmsToBM (dbms: DBM[], opts: CommonDaoOptions = {}): Promise<BM[]> {
    return Promise.all(dbms.map(dbm => this.dbmToBM(dbm, opts)))
  }

  /**
   * Mutates object with properties: id, created, updated.
   * Returns DBM (new reference).
   */
  async bmToDBM (unsavedBM: Unsaved<BM>, opts?: CommonDaoOptions): Promise<DBM> {
    if (unsavedBM === undefined) return undefined as any

    // Validate/convert BM
    // bm gets assigned to the new reference
    unsavedBM = this.validateAndConvert(unsavedBM, this.cfg.bmUnsavedSchema, DBModelType.BM, opts)

    // BM > DBM
    let dbm = await this.beforeBMToDBM(unsavedBM)

    // Does not mutate
    dbm = this.assignIdCreatedUpdated(dbm, opts)

    // Validate/convert DBM
    return this.validateAndConvert(dbm, this.dbmSavedSchema, DBModelType.DBM, opts)
  }

  async bmsToDBM (bms: Unsaved<BM>[], opts: CommonDaoOptions = {}): Promise<DBM[]> {
    // try/catch?
    return Promise.all(bms.map(bm => this.bmToDBM(bm, opts)))
  }

  anyToDBM (_dbm: Unsaved<DBM>, opts: CommonDaoOptions = {}): DBM {
    if (!_dbm) return undefined as any

    let dbm = this.assignIdCreatedUpdated(_dbm, opts)
    dbm = { ...dbm, ...this.parseNaturalId(dbm.id) }

    // Validate/convert DBM
    return this.validateAndConvert<DBM>(dbm, this.dbmSavedSchema, DBModelType.DBM, opts)
  }

  anyToDBMs (entities: Unsaved<DBM>[], opts: CommonDaoOptions = {}): DBM[] {
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

  protected logResult (started: number, op: string, res: any): void {
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
