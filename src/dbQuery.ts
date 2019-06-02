export type DBQueryFilterOperator = '<' | '<=' | '=' | '>=' | '>'

export interface DBQueryFilter {
  name: string
  op: DBQueryFilterOperator
  val: any
}

export interface DBQueryOrder {
  name: string
  descending?: boolean
}

/**
 * Lowest Common Denominator Query object.
 * To be executed by CommonDao / CommonDB.
 *
 * Fluent API (returns `this` after each method).
 *
 * <DBM> is the type of **queried** object (so e.g `key of DBM` can be used), not **returned** object.
 */
export class DBQuery<DBM = any> {
  constructor (public kind: string) {}

  _filters: DBQueryFilter[] = []
  _limitValue = 0 // 0 means "no limit"
  _orders: DBQueryOrder[] = []
  /**
   * If defined - only those fields will be selected.
   * In undefined - all fields (*) will be returned.
   */
  _selectedFieldNames?: string[]

  filter (name: string, op: DBQueryFilterOperator, val: any): this {
    this._filters.push({ name, op, val })
    return this
  }

  limit (limit: number): this {
    this._limitValue = limit
    return this
  }

  order (name: string, descending?: boolean): this {
    this._orders.push({
      name,
      descending,
    })
    return this
  }

  select (fieldNames: string[]): this {
    this._selectedFieldNames = fieldNames
    return this
  }
}
