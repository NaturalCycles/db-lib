import {
  AnyObjectWithId,
  ObjectWithId,
  AsyncMapper,
  _truncate,
  Saved,
  AnyObject,
  _objectAssign,
} from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import {
  CommonDaoOptions,
  CommonDaoStreamDeleteOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
  DBPatch,
} from '..'
import { CommonDao } from '../commondao/common.dao'
import { RunQueryResult } from '../db.model'

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
  | '!='
  | '>='
  | '>'
  | 'in'
  | 'not-in'
  | 'array-contains'
  | 'array-contains-any'

export const dbQueryFilterOperatorValues: DBQueryFilterOperator[] = [
  '<',
  '<=',
  '==',
  '!=',
  '>=',
  '>',
  'in',
  'not-in',
  'array-contains',
  'array-contains-any',
]

export interface DBQueryFilter<ROW extends ObjectWithId = AnyObjectWithId> {
  name: keyof ROW
  op: DBQueryFilterOperator
  val: any
}

export interface DBQueryOrder<ROW extends ObjectWithId = AnyObjectWithId> {
  name: keyof ROW
  descending?: boolean
}

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
export class DBQuery<ROW extends ObjectWithId = AnyObjectWithId> {
  constructor(public table: string) {}

  /**
   * Convenience method.
   */
  static create<ROW extends ObjectWithId = AnyObjectWithId>(table: string): DBQuery<ROW> {
    return new DBQuery(table)
  }

  static fromPlainObject<ROW extends ObjectWithId = AnyObjectWithId>(
    obj: Partial<DBQuery<ROW>> & { table: string },
  ): DBQuery<ROW> {
    return Object.assign(new DBQuery<ROW>(obj.table), obj)
  }

  _filters: DBQueryFilter<ROW>[] = []
  _limitValue = 0 // 0 means "no limit"
  _offsetValue = 0 // 0 means "no offset"
  _orders: DBQueryOrder<ROW>[] = []

  _startCursor?: string
  _endCursor?: string

  /**
   * If defined - only those fields will be selected.
   * In undefined - all fields (*) will be returned.
   */
  _selectedFieldNames?: (keyof ROW)[]
  _groupByFieldNames?: (keyof ROW)[]
  _distinct = false

  filter(name: keyof ROW, op: DBQueryFilterOperator, val: any): this {
    this._filters.push({ name, op, val })
    return this
  }

  filterEq(name: keyof ROW, val: any): this {
    this._filters.push({ name, op: '==', val })
    return this
  }

  filterIn(name: keyof ROW, val: any[]): this {
    this._filters.push({ name, op: 'in', val })
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

  order(name: keyof ROW, descending?: boolean): this {
    this._orders.push({
      name,
      descending,
    })
    return this
  }

  select(fieldNames: (keyof ROW)[]): this {
    this._selectedFieldNames = fieldNames
    return this
  }

  groupBy(fieldNames: (keyof ROW)[]): this {
    this._groupByFieldNames = fieldNames
    return this
  }

  distinct(distinct = true): this {
    this._distinct = distinct
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
    return _objectAssign(new DBQuery<ROW>(this.table), {
      _filters: [...this._filters],
      _limitValue: this._limitValue,
      _offsetValue: this._offsetValue,
      _orders: [...this._orders],
      _selectedFieldNames: this._selectedFieldNames && [...this._selectedFieldNames],
      _groupByFieldNames: this._groupByFieldNames && [...this._groupByFieldNames],
      _distinct: this._distinct,
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
      tokens.push(
        `select${this._distinct ? ' distinct' : ''}(${this._selectedFieldNames.join(',')})`,
      )
    }

    tokens.push(
      ...this._filters.map(f => `${f.name as string}${f.op}${f.val}`),
      ...this._orders.map(o => `order by ${o.name as string}${o.descending ? ' desc' : ''}`),
    )

    if (this._groupByFieldNames) {
      tokens.push(`groupBy(${this._groupByFieldNames.join(',')})`)
    }

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
  BM extends Partial<ObjectWithId<ID>>,
  DBM extends ObjectWithId<ID> = Saved<BM>,
  TM extends AnyObject = BM,
  ID extends string | number = string,
> extends DBQuery<DBM> {
  /**
   * Pass `table` to override table.
   */
  constructor(
    public dao: CommonDao<BM, DBM, TM, ID>,
    table?: string,
  ) {
    super(table || dao.cfg.table)
  }

  async runQuery(opt?: CommonDaoOptions): Promise<Saved<BM>[]> {
    return await this.dao.runQuery(this, opt)
  }

  async runQuerySingleColumn<T = any>(opt?: CommonDaoOptions): Promise<T[]> {
    return await this.dao.runQuerySingleColumn<T>(this, opt)
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

  async updateByQuery(patch: DBPatch<DBM>, opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.updateByQuery(this, patch, opt)
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

  streamQuery(opt?: CommonDaoStreamOptions<Saved<BM>>): ReadableTyped<Saved<BM>> {
    return this.dao.streamQuery(this, opt)
  }

  streamQueryAsDBM(opt?: CommonDaoStreamOptions<DBM>): ReadableTyped<DBM> {
    return this.dao.streamQueryAsDBM(this, opt)
  }

  async queryIds(opt?: CommonDaoOptions): Promise<ID[]> {
    return await this.dao.queryIds(this, opt)
  }

  streamQueryIds(opt?: CommonDaoStreamOptions<ID>): ReadableTyped<ID> {
    return this.dao.streamQueryIds(this, opt)
  }

  async streamQueryIdsForEach(
    mapper: AsyncMapper<ID, void>,
    opt?: CommonDaoStreamForEachOptions<ID>,
  ): Promise<void> {
    await this.dao.streamQueryIdsForEach(this, mapper, opt)
  }

  async deleteByQuery(opt?: CommonDaoStreamDeleteOptions<DBM>): Promise<number> {
    return await this.dao.deleteByQuery(this, opt)
  }
}
