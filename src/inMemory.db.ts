import { _pick } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { Observable, of } from 'rxjs'
import { CommonDBOptions, CommonDBSaveOptions, RunQueryResult, SavedDBEntity } from './db.model'
import { DBQuery, DBQueryFilterOperator } from './dbQuery'
import { CommonDB } from './index'

type FilterFn = (v: any, val: any) => boolean
const FILTER_FNS: Record<DBQueryFilterOperator, FilterFn> = {
  '=': (v, val) => v === val,
  '<': (v, val) => v < val,
  '<=': (v, val) => v <= val,
  '>': (v, val) => v > val,
  '>=': (v, val) => v >= val,
  in: (v, val) => ((val as any[]) || []).includes(v),
}

const log = Debug('nc:db-lib:inmemorydb')

export class InMemoryDB implements CommonDB {
  // Table > id > row
  data: Record<string, Record<string, any>> = {}

  /**
   * Resets InMemory DB data
   */
  async resetCache(table?: string): Promise<void> {
    if (table) {
      log(`reset ${table}`)
      this.data[table] = {}
    } else {
      log('reset')
      this.data = {}
    }
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    this.data[table] = this.data[table] || {}
    return ids.map(id => this.data[table][id]).filter(Boolean)
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    this.data[table] = this.data[table] || {}

    dbms.forEach(dbm => {
      if (!dbm.id) {
        log.warn({ dbms })
        throw new Error(`InMemoryDB: id doesn't exist for record`)
      }
      this.data[table][dbm.id] = dbm
    })
  }

  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    this.data[table] = this.data[table] || {}

    return ids
      .map(id => {
        const exists = !!this.data[table][id]
        delete this.data[table][id]
        if (exists) return id
      })
      .filter(Boolean).length
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const rows = queryInMemory(q, Object.values(this.data[q.table] || {}))
    const ids = rows.map(r => r.id)
    return this.deleteByIds(q.table, ids)
  }

  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<RunQueryResult<DBM>> {
    return { records: queryInMemory(q, Object.values(this.data[q.table] || {})) }
  }

  async runQueryCount<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    return queryInMemory(q, Object.values(this.data[q.table] || {})).length
  }

  streamQuery<DBM extends SavedDBEntity>(q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    return of(...queryInMemory(q, Object.values(this.data[q.table] || {})))
  }
}

export function queryInMemory<DBM extends SavedDBEntity>(q: DBQuery<DBM>, rows: DBM[] = []): DBM[] {
  // .filter
  rows = q._filters.reduce((rows, filter) => {
    return rows.filter(row => {
      const fn = FILTER_FNS[filter.op]
      if (!fn) throw new Error(`InMemoryDB query filter op not supported: ${filter.op}`)
      return fn(row[filter.name], filter.val)
    })
  }, rows)

  // .select(fieldNames)
  if (q._selectedFieldNames) {
    rows = rows.map(
      row =>
        _pick(row, q._selectedFieldNames!.length ? q._selectedFieldNames : (['id'] as any)) as DBM,
    )
  }

  // todo: only one order is supported (first)
  const [order] = q._orders
  if (order) {
    const { name, descending } = order
    rows = rows.sort((a, b) => {
      // tslint:disable-next-line:triple-equals
      if (a[name] == b[name]) return 0

      if (descending) {
        return a[name] < b[name] ? 1 : -1
      } else {
        return a[name] > b[name] ? 1 : -1
      }
    })
  }

  // .limit()
  if (q._limitValue) {
    rows = rows.slice(0, Math.min(q._limitValue, rows.length))
  }

  return rows as DBM[]
}
