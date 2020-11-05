import { AsyncMapper, _truncate } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDaoOptions, CommonDaoStreamForEachOptions, CommonDaoStreamOptions } from '..'
import { CommonDao } from '../commondao/common.dao'
import { ObjectWithId, RunQueryResult, Saved } from '../db.model'

/**
 * Modeled after Firestore operators (WhereFilterOp type)
 *
 * As explained in https://firebase.google.com/docs/firestore/query-data/queries
 *
 * 'array-contains' applies to the field of type ARRAY, returns a doc if the array contains the given value,
 * e.g .filter('languages', 'array-contains', 'en')
 * where 'languages' can be e.g ['en', 'sv']
 *
 * 'in' applies to a non-ARRAY fields, but allows to pass multiple values to compare with, e.g:
 * .filter('lang', 'in', ['en', 'sv'])
 * will returns users that have EITHER en OR sv in their language
 *
 * 'array-contains-any' applies to ARRAY field and ARRAY of given arguments,
 * works like an "intersection". Returns a document if intersection is not empty, e.g:
 * .filter('languages', 'array-contains-any', ['en', 'sv'])
 *
 * You may also look at queryInMemory() for its implementation (it implements all those).
 */
export type DBQueryFilterOperator =
  | '<'
  | '<='
  | '=='
  | '>='
  | '>'
  | 'in'
  | 'not-in'
  | 'array-contains'
  | 'array-contains-any'

export const DBQueryFilterOperatorValues = [
  '<',
  '<=',
  '==',
  '>=',
  '>',
  'in',
  'not-in',
  'array-contains',
  'array-contains-any',
]

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
export class DBQuery<ROW extends ObjectWithId> {
  constructor(public table: string) {}

  /**
   * Convenience method.
   */
  static create<ROW extends ObjectWithId = any>(table: string): DBQuery<ROW> {
    return new DBQuery(table)
  }

  static fromPlainObject<ROW extends ObjectWithId = any>(
    obj: Partial<DBQuery<ROW>> & { table: string },
  ): DBQuery<ROW> {
    return Object.assign(new DBQuery<ROW>(obj.table), obj)
  }

  _filters: DBQueryFilter[] = []
  _limitValue = 0 // 0 means "no limit"
  _offsetValue = 0 // 0 means "no offset"
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
    this._filters.push({ name, op: '==', val })
    return this
  }

  limit(limit: number): this {
    this._limitValue = limit
    return this
  }

  offset(offset: number): this {
    this._offsetValue = offset
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

  clone(): DBQuery<ROW> {
    return Object.assign(new DBQuery<ROW>(this.table), {
      _filters: [...this._filters],
      _limitValue: this._limitValue,
      _offsetValue: this._offsetValue,
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

    // if (this.name) {
    //   tokens.push(`"${this.name}"`)
    // }

    if (this._selectedFieldNames) {
      tokens.push(`select(${this._selectedFieldNames.join(',')})`)
    }

    tokens.push(...this._filters.map(f => `${f.name}${f.op}${f.val}`))

    tokens.push(...this._orders.map(o => `order by ${o.name}${o.descending ? ' desc' : ''}`))

    if (this._offsetValue) {
      tokens.push(`offset ${this._offsetValue}`)
    }

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
  BM extends Partial<ObjectWithId>,
  DBM extends ObjectWithId,
  TM
> extends DBQuery<DBM> {
  /**
   * Pass `table` to override table.
   */
  constructor(public dao: CommonDao<BM, DBM, TM>, table?: string) {
    super(table || dao.cfg.table)
  }

  async runQuery(opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.dao.runQuery(this, opt)
  }

  async runQueryAsDBM(opt?: CommonDaoOptions): Promise<DBM[]> {
    return await this.dao.runQueryAsDBM(this, opt)
  }

  async runQueryAsTM(opt?: CommonDaoOptions): Promise<TM[]> {
    return await this.dao.runQueryAsTM(this, opt)
  }

  async runQueryExtended(opt?: CommonDaoOptions): Promise<RunQueryResult<Saved<BM>>> {
    return await this.dao.runQueryExtended(this, opt)
  }

  async runQueryExtendedAsDBM(opt?: CommonDaoOptions): Promise<RunQueryResult<DBM>> {
    return await this.dao.runQueryExtendedAsDBM(this, opt)
  }

  async runQueryExtendedAsTM(opt?: CommonDaoOptions): Promise<RunQueryResult<TM>> {
    return await this.dao.runQueryExtendedAsTM(this, opt)
  }

  async runQueryCount(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.runQueryCount(this, opt)
  }

  async streamQueryForEach(
    mapper: AsyncMapper<Saved<BM>, void>,
    opt?: CommonDaoStreamForEachOptions<Saved<BM>>,
  ): Promise<void> {
    await this.dao.streamQueryForEach(this, mapper, opt)
  }

  async streamQueryAsDBMForEach(
    mapper: AsyncMapper<DBM, void>,
    opt?: CommonDaoStreamForEachOptions<DBM>,
  ): Promise<void> {
    await this.dao.streamQueryAsDBMForEach(this, mapper, opt)
  }

  streamQuery(opt?: CommonDaoStreamOptions<DBM, Saved<BM>>): ReadableTyped<Saved<BM>> {
    return this.dao.streamQuery(this, opt)
  }

  streamQueryAsDBM(opt?: CommonDaoStreamOptions<any, DBM>): ReadableTyped<DBM> {
    return this.dao.streamQueryAsDBM(this, opt)
  }

  async queryIds(opt?: CommonDaoOptions): Promise<string[]> {
    return await this.dao.queryIds(this, opt)
  }

  streamQueryIds(opt?: CommonDaoStreamOptions<ObjectWithId, string>): ReadableTyped<string> {
    return this.dao.streamQueryIds(this, opt)
  }

  async streamQueryIdsForEach(
    mapper: AsyncMapper<string, void>,
    opt?: CommonDaoStreamForEachOptions<string>,
  ): Promise<void> {
    await this.dao.streamQueryIdsForEach(this, mapper, opt)
  }

  async deleteByQuery(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.deleteByQuery(this, opt)
  }
}
