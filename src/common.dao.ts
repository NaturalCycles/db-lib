import {
  getValidationResult,
  JoiValidationError,
  ObjectSchemaTyped,
} from '@naturalcycles/nodejs-lib'
import { Observable } from 'rxjs'
import { map, mergeMap } from 'rxjs/operators'
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

export interface CommonDaoCfg<BM, DBM, DB extends CommonDB = CommonDB> {
  db: DB
  table: string
  dbmSchema?: ObjectSchemaTyped<DBM>
  bmSchema?: ObjectSchemaTyped<BM>

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

/**
 * Lowest common denominator API between supported Databases.
 *
 * DBM = Database model (how it's stored in DB)
 * BM = Backend model (optimized for API access)
 */
export class CommonDao<BM extends BaseDBEntity = any, DBM extends BaseDBEntity = BM> {
  constructor (protected cfg: CommonDaoCfg<BM, DBM>) {}

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
    const [dbm] = await this.cfg.db.getByIds(this.cfg.table, [id])
    return this.dbmToBM(dbm, opts)
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
    const [entity] = await this.cfg.db.getByIds(this.cfg.table, [id])
    return this.anyToDBM(entity, opts)
  }

  async getByIds (ids: string[], opts?: CommonDaoOptions): Promise<BM[]> {
    const dbms = await this.cfg.db.getByIds(this.cfg.table, ids)
    return this.dbmsToBM(dbms, opts)
  }

  async requireById (id: string, opts?: CommonDaoOptions): Promise<BM> {
    const r = await this.getById(id, opts)
    if (!r) throw new Error(`DB record required, but not found: ${this.cfg.table}.${id}`)
    return r
  }

  async getBy (by: string, value: any, limit = 0, opts?: CommonDaoOptions): Promise<BM[]> {
    const q = this.createQuery(this.cfg.table)
      .filter(by, '=', value)
      .limit(limit)
    return this.runQuery(q, opts)
  }

  async getOneBy (by: string, value: any, opts?: CommonDaoOptions): Promise<BM | undefined> {
    const q = this.createQuery(this.cfg.table)
      .filter(by, '=', value)
      .limit(1)
    const [bm] = await this.runQuery(q, opts)
    return bm
  }

  // QUERY
  createQuery (table: string): DBQuery<DBM> {
    return new DBQuery<DBM>(table)
  }

  async runQuery (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<BM[]> {
    const dbms = await this.cfg.db.runQuery(q, opts)
    return this.dbmsToBM(dbms, opts)
  }

  async runQueryAsDBM (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<DBM[]> {
    const entities = await this.cfg.db.runQuery(q, opts)
    return this.anyToDBMs(entities, opts)
  }

  async runQueryCount (q: DBQuery<DBM>, opts?: CommonDaoOptions): Promise<number> {
    return this.cfg.db.runQueryCount(q, opts)
  }

  streamQuery (q: DBQuery<DBM>, opts?: CommonDaoOptions): Observable<BM> {
    return this.cfg.db.streamQuery(q).pipe(mergeMap(dbm => this.dbmToBM(dbm, opts)))
  }

  streamQueryAsDBM (q: DBQuery<DBM>, opts?: CommonDaoOptions): Observable<DBM> {
    return this.cfg.db.streamQuery(q).pipe(map(entity => this.anyToDBM(entity, opts)))
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
    const [savedBM] = await this.saveBatch([bm], opts)
    return savedBM
  }

  async saveAsDBM (dbm: DBM, opts?: CommonDaoSaveOptions): Promise<DBM> {
    const [savedDBM] = await this.saveBatchAsDBM([dbm], opts)
    return savedDBM
  }

  async saveBatch (bms: BM[], opts?: CommonDaoSaveOptions): Promise<BM[]> {
    const dbms = await this.bmsToDBM(bms, opts)
    const savedDBMs = await this.cfg.db.saveBatch(this.cfg.table, dbms, opts)
    return this.dbmsToBM(savedDBMs, opts)
  }

  async saveBatchAsDBM (_dbms: DBM[], opts?: CommonDaoSaveOptions): Promise<DBM[]> {
    // anyToDBMs
    const dbms = this.anyToDBMs(_dbms, opts)

    const savedDBMs = await this.cfg.db.saveBatch(this.cfg.table, dbms, opts)
    return this.anyToDBMs(savedDBMs, opts)
  }

  // DELETE
  /**
   * @returns array of deleted items' ids
   */
  async deleteById (id?: string): Promise<string[]> {
    if (!id) return []
    return this.cfg.db.deleteByIds(this.cfg.table, [id])
  }

  async deleteByIds (ids: string[]): Promise<string[]> {
    return this.cfg.db.deleteByIds(this.cfg.table, ids)
  }

  async deleteBy (by: string, value: any, limit = 0, opts?: CommonDaoOptions): Promise<string[]> {
    // const q = this.createQuery(this.cfg.table).filter(by, '=', value).limit(limit).select(['id'])
    // const ids = await this.cfg.db.runQuery<string>(q, opts)
    // return this.cfg.db.deleteByIds(this.cfg.table, ids)
    return this.cfg.db.deleteBy(this.cfg.table, by, value, limit, opts)
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
