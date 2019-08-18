import { _pick, StringMap } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { Observable, of } from 'rxjs'
import { BaseDBEntity, CommonDB, CommonDBOptions, CommonDBSaveOptions } from './db.model'
import { DBQuery } from './dbQuery'

type FilterFn = (v: any, val: any) => boolean
const FILTER_FNS: StringMap<FilterFn> = {
  '=': (v, val) => v === val,
  '<': (v, val) => v < val,
  '<=': (v, val) => v <= val,
  '>': (v, val) => v > val,
  '>=': (v, val) => v >= val,
}

const log = Debug('nc:db-lib:inmemorydb')

export class InMemoryDB<DBM extends BaseDBEntity> implements CommonDB<DBM> {
  // Table > id > row
  data: StringMap<StringMap<any>> = {}

  /**
   * Resets InMemory DB data
   */
  async resetCache (): Promise<void> {
    log('reset')
    this.data = {}
  }

  async getByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<DBM[]> {
    this.data[table] = this.data[table] || {}
    return ids.map(id => this.data[table][id]).filter(Boolean)
  }

  async saveBatch (table: string, dbms: DBM[], opts?: CommonDBSaveOptions): Promise<DBM[]> {
    this.data[table] = this.data[table] || {}

    return dbms.map(dbm => {
      if (!dbm.id) {
        log.warn({ dbms })
        throw new Error(`InMemoryDB: id doesn't exist for record`)
      }
      this.data[table][dbm.id] = dbm
      return dbm
    })
  }

  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]> {
    this.data[table] = this.data[table] || {}

    return ids
      .map(id => {
        const exists = !!this.data[table][id]
        delete this.data[table][id]
        if (exists) return id
      })
      .filter(Boolean) as string[]
  }

  async deleteByQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<string[]> {
    const rows = queryInMemory(q, this.data[q.table])
    const ids = rows.map(r => r.id)
    return this.deleteByIds(q.table, ids)
  }

  async runQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]> {
    return queryInMemory(q, this.data[q.table])
  }

  async runQueryCount (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number> {
    return queryInMemory(q, this.data[q.table]).length
  }

  streamQuery (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    return of(...queryInMemory(q, this.data[q.table]))
  }
}

export function queryInMemory<DBM> (q: DBQuery<DBM>, tableCache?: StringMap<DBM>): DBM[] {
  let rows: any[] = Object.values(tableCache || [])

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
    rows = rows.map(row =>
      _pick(row, q._selectedFieldNames!.length ? q._selectedFieldNames : ['id']),
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
