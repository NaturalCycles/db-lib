import { AsyncMapper, _truncate } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDaoOptions, CommonDaoStreamForEachOptions, CommonDaoStreamOptions } from '..'
import { CommonDao } from '../commondao/common.dao'
import { BaseDBEntity, ObjectWithId, RunQueryResult, Saved, SavedDBEntity } from '../db.model'

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
export class DBQuery<ROW extends ObjectWithId = any> {
  constructor(public table: string) {}

  /**
   * Convenience method.
   */
  static create<ROW extends ObjectWithId = any>(table: string): DBQuery<ROW> {
    return new DBQuery(table)
  }

  static fromPlainObject<ROW extends ObjectWithId = any>(
    obj: Partial<DBQuery> & { table: string },
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
    this._filters.push({ name, op: '=', val })
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
  BM extends BaseDBEntity,
  DBM extends SavedDBEntity,
  TM
> extends DBQuery<DBM> {
  /**
   * Pass `table` to override table.
   */
  constructor(public dao: CommonDao<BM, DBM, TM>, table?: string) {
    super(table || dao.cfg.table)
  }

  async runQuery<OUT = Saved<BM>>(opt?: CommonDaoOptions): Promise<OUT[]> {
    return await this.dao.runQuery<OUT>(this, opt)
  }

  async runQueryAsDBM<OUT = DBM>(opt?: CommonDaoOptions): Promise<OUT[]> {
    return await this.dao.runQueryAsDBM<OUT>(this, opt)
  }

  async runQueryExtended<OUT = Saved<BM>>(opt?: CommonDaoOptions): Promise<RunQueryResult<OUT>> {
    return await this.dao.runQueryExtended<OUT>(this, opt)
  }

  async runQueryExtendedAsDBM<OUT = DBM>(opt?: CommonDaoOptions): Promise<RunQueryResult<OUT>> {
    return await this.dao.runQueryExtendedAsDBM<OUT>(this, opt)
  }

  async runQueryCount(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.runQueryCount(this, opt)
  }

  async streamQueryForEach<IN = Saved<BM>, OUT = IN>(
    mapper: AsyncMapper<OUT, void>,
    opt?: CommonDaoStreamForEachOptions,
  ): Promise<void> {
    await this.dao.streamQueryForEach<IN, OUT>(this, mapper, opt)
  }

  async streamQueryAsDBMForEach<IN = DBM, OUT = IN>(
    mapper: AsyncMapper<OUT, void>,
    opt?: CommonDaoStreamForEachOptions,
  ): Promise<void> {
    await this.dao.streamQueryAsDBMForEach<IN, OUT>(this, mapper, opt)
  }

  streamQuery<OUT = Saved<BM>>(opt?: CommonDaoStreamOptions): ReadableTyped<OUT> {
    return this.dao.streamQuery<OUT>(this, opt)
  }

  streamQueryAsDBM<OUT = DBM>(opt?: CommonDaoStreamOptions): ReadableTyped<OUT> {
    return this.dao.streamQueryAsDBM<OUT>(this, opt)
  }

  async queryIds(opt?: CommonDaoOptions): Promise<string[]> {
    return await this.dao.queryIds(this, opt)
  }

  streamQueryIds(opt?: CommonDaoStreamOptions): ReadableTyped<string> {
    return this.dao.streamQueryIds(this, opt)
  }

  async streamQueryIdsForEach(
    mapper: AsyncMapper<string, void>,
    opt?: CommonDaoStreamForEachOptions,
  ): Promise<void> {
    await this.dao.streamQueryIdsForEach(this, mapper, opt)
  }

  async deleteByQuery(opt?: CommonDaoOptions): Promise<number> {
    return await this.dao.deleteByQuery(this, opt)
  }
}
