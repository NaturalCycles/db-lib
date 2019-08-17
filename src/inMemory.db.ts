import { _pick, StringMap } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { Observable, Subject } from 'rxjs'
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

export class InMemoryDB implements CommonDB {
  // Table > id > row
  data: StringMap<StringMap<any>> = {}

  /**
   * Resets InMemory DB data
   */
  reset (): void {
    log('reset')
    this.data = {}
  }

  async getByIds<DBM = any> (table: string, ids: string[], opts?: CommonDBOptions): Promise<DBM[]> {
    return ids.map<DBM>(id => (this.data[table] || {})[id]).filter(Boolean)
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<DBM[]> {
    this.data[table] = this.data[table] || {}

    return dbms.map(dbm => {
      if (!dbm.id) {
        console.warn({ dbms })
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

  async deleteBy (
    table: string,
    by: string,
    value: any,
    limit?: number,
    opts?: CommonDBOptions,
  ): Promise<string[]> {
    this.data[table] = this.data[table] || {}

    const ids = Object.entries(this.data[table])
      .map(([id, row]) => {
        if (row[by] === value) return id
      })
      .filter(Boolean) as string[]

    return this.deleteByIds(table, ids)
  }

  async runQuery<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]> {
    const { table } = q

    let rows: any[] = Object.values(this.data[table] || [])

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

  async runQueryCount<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number> {
    const rows = await this.runQuery(q)
    return rows.length
  }

  streamQuery<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    const subj = new Subject<DBM>()

    this.runQuery<DBM>(q)
      .then(rows => {
        rows.forEach(row => subj.next(row))
        subj.complete()
      })
      .catch(err => {
        subj.error(err)
      })

    return subj
  }
}
