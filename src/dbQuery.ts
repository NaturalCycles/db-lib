import { _truncate } from '@naturalcycles/js-lib'
import { Observable } from 'rxjs'
import { CommonDao } from './common.dao'
import { BaseDBEntity, CommonDaoOptions, RunQueryResult, Saved, SavedDBEntity } from './db.model'

export type DBQueryFilterOperator = '<' | '<=' | '=' | '>=' | '>' | 'in'

export interface DBQueryFilter {
  name: string
  op: DBQueryFilterOperator
  val: any
}

export interface DBQueryOrder {
  name: string
  descending?: boolean
}

// export interface DBQueryData {
//   _filters: DBQueryFilter[]
//   _limitValue: number
//   _orders: DBQueryOrder[]
//
//   /**
//    * If defined - only those fields will be selected.
//    * In undefined - all fields (*) will be returned.
//    */
//   _selectedFieldNames?: string[]
// }

/**
 * Lowest Common Denominator Query object.
 * To be executed by CommonDao / CommonDB.
 *
 * Fluent API (returns `this` after each method).
 *
 * Methods do MUTATE the query object, be careful.
 *
 * <DBM> is the type of **queried** object (so e.g `key of DBM` can be used), not **returned** object.
 */
export class DBQuery<DBM = any> {
  constructor(public table: string, public name?: string) {}

  _filters: DBQueryFilter[] = []
  _limitValue = 0 // 0 means "no limit"
  _orders: DBQueryOrder[] = []

  _startCursor?: string
  _endCursor?: string

  /**
   * If defined - only those fields will be selected.
   * In undefined - all fields (*) will be returned.
   */
  _selectedFieldNames?: string[]

  filter(name: string, op: DBQueryFilterOperator, val: any): this {
    this._filters.push({ name, op, val })
    return this
  }

  filterEq(name: string, val: any): this {
    this._filters.push({ name, op: '=', val })
    return this
  }

  limit(limit: number): this {
    this._limitValue = limit
    return this
  }

  order(name: string, descending?: boolean): this {
    this._orders.push({
      name,
      descending,
    })
    return this
  }

  select(fieldNames: string[]): this {
    this._selectedFieldNames = fieldNames
    return this
  }

  startCursor(startCursor?: string): this {
    this._startCursor = startCursor
    return this
  }

  endCursor(endCursor?: string): this {
    this._endCursor = endCursor
    return this
  }

  clone(): DBQuery<DBM> {
    return Object.assign(new DBQuery<DBM>(this.table), {
      _filters: [...this._filters],
      _limitValue: this._limitValue,
      _orders: [...this._orders],
      _selectedFieldNames: this._selectedFieldNames && [...this._selectedFieldNames],
      _startCursor: this._startCursor,
      _endCursor: this._endCursor,
    })
  }

  pretty(): string {
    return this.prettyConditions().join(', ')
  }

  prettyConditions(): string[] {
    const tokens = []

    if (this.name) {
      tokens.push(`"${this.name}"`)
    }

    if (this._selectedFieldNames) {
      tokens.push(`select(${this._selectedFieldNames.join(',')})`)
    }

    tokens.push(...this._filters.map(f => `${f.name}${f.op}${f.val}`))

    tokens.push(...this._orders.map(o => `order by ${o.name}${o.descending ? ' desc' : ''}`))

    if (this._limitValue) {
      tokens.push(`limit ${this._limitValue}`)
    }

    if (this._startCursor) {
      tokens.push(`startCursor ${_truncate(this._startCursor, 8)}`)
    }

    if (this._endCursor) {
      tokens.push(`endCursor ${_truncate(this._endCursor, 8)}`)
    }

    return tokens
  }
}

/**
 * DBQuery that has additional method to support Fluent API style.
 */
export class RunnableDBQuery<
  BM extends BaseDBEntity = any,
  DBM extends SavedDBEntity = Saved<BM>,
  TM = BM
> extends DBQuery<DBM> {
  constructor(public dao: CommonDao<BM, DBM, TM>, name?: string) {
    super(dao.cfg.table, name)
  }

  async runQuery(opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.dao.runQuery(this, opt)
  }

  async runAsDBM(opt?: CommonDaoOptions): Promise<DBM[]> {
    return await this.dao.runQueryAsDBM(this, opt)
  }

  async runExtended(opt?: CommonDaoOptions): Promise<RunQueryResult<Saved<BM>>> {
    return await this.dao.runQueryExtended(this, opt)
  }

  async runExtendedAsDBM(opt?: CommonDaoOptions): Promise<RunQueryResult<DBM>> {
    return await this.dao.runQueryExtendedAsDBM(this, opt)
  }

  async runQueryCount(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.runQueryCount(this, opt)
  }

  streamQuery(opt?: CommonDaoOptions): Observable<Saved<BM>> {
    return this.dao.streamQuery(this, opt)
  }

  streamQueryAsDBM(opt?: CommonDaoOptions): Observable<DBM> {
    return this.dao.streamQueryAsDBM(this, opt)
  }

  async queryIds(opt?: CommonDaoOptions): Promise<string[]> {
    return await this.dao.queryIds(this, opt)
  }

  streamQueryIds(opt?: CommonDaoOptions): Observable<string> {
    return this.dao.streamQueryIds(this, opt)
  }

  async deleteByQuery(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.deleteByQuery(this, opt)
  }
}
