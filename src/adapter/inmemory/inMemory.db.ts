import { _pick } from '@naturalcycles/js-lib'
import { Debug, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { Readable } from 'stream'
import { CommonDB } from '../..'
import { CommonSchema } from '../..'
import { CommonSchemaGenerator } from '../..'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  RunQueryResult,
  SavedDBEntity,
} from '../../db.model'
import { DBQuery, DBQueryFilterOperator } from '../../dbQuery'

export interface InMemoryDBCfg {
  /**
   * @default ''
   *
   * Allows to support "Namespacing".
   * E.g, pass `ns1_` to it and all tables will be prefixed by it.
   * Reset cache respects this prefix (won't touch other namespaces!)
   */
  tablesPrefix: string
}

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
  constructor(cfg?: Partial<InMemoryDBCfg>) {
    this.cfg = {
      // defaults
      tablesPrefix: '',
      ...cfg,
    }
  }

  cfg: InMemoryDBCfg

  // Table > id > row
  data: Record<string, Record<string, any>> = {}

  async ping(): Promise<void> {}

  /**
   * Resets InMemory DB data
   */
  async resetCache(_table?: string): Promise<void> {
    if (_table) {
      const table = this.cfg.tablesPrefix + _table
      log(`reset ${table}`)
      this.data[table] = {}
    } else {
      ;(await this.getTables()).forEach(table => {
        this.data[table] = {}
      })
      log('reset')
    }
  }

  async getTables(): Promise<string[]> {
    return Object.keys(this.data).filter(t => t.startsWith(this.cfg.tablesPrefix))
  }

  async getTableSchema<DBM>(_table: string): Promise<CommonSchema<DBM>> {
    const table = this.cfg.tablesPrefix + _table
    return CommonSchemaGenerator.generateFromRows<DBM>(
      { table },
      Object.values(this.data[table] || {}),
    )
  }

  async createTable(schema: CommonSchema, opt: CommonDBCreateOptions = {}): Promise<void> {
    const table = this.cfg.tablesPrefix + schema.table
    if (opt.dropIfExists) {
      this.data[table] = {}
    } else {
      this.data[table] = this.data[table] || {}
    }
  }

  async getByIds<DBM extends SavedDBEntity>(
    _table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<DBM[]> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}
    return ids.map(id => this.data[table][id]).filter(Boolean)
  }

  async saveBatch<DBM extends SavedDBEntity>(
    _table: string,
    dbms: DBM[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {
    const table = this.cfg.tablesPrefix + _table
    this.data[table] = this.data[table] || {}

    dbms.forEach(dbm => {
      if (!dbm.id) {
        log.warn({ dbms })
        throw new Error(`InMemoryDB: id doesn't exist for record`)
      }
      this.data[table][dbm.id] = dbm
    })
  }

  async deleteByIds(_table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + _table
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
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    const rows = queryInMemory<DBM>(q, Object.values(this.data[table] || {}))
    const ids = rows.map(r => r.id)
    return this.deleteByIds(q.table, ids)
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    const table = this.cfg.tablesPrefix + q.table
    return { records: queryInMemory<DBM, OUT>(q, Object.values(this.data[table] || {})) }
  }

  async runQueryCount(q: DBQuery, opt?: CommonDBOptions): Promise<number> {
    const table = this.cfg.tablesPrefix + q.table
    return queryInMemory(q, Object.values(this.data[table] || {})).length
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt?: CommonDBOptions,
  ): ReadableTyped<OUT> {
    const table = this.cfg.tablesPrefix + q.table
    return Readable.from(queryInMemory<DBM, OUT>(q, Object.values(this.data[table] || {})))
  }
}

// Important: q.table is not used in this function, so tablesPrefix is not needed.
// But should be careful here..
export function queryInMemory<DBM extends SavedDBEntity, OUT = DBM>(
  q: DBQuery<any, DBM>,
  rows: DBM[] = [],
): OUT[] {
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

  // .offset()
  if (q._offsetValue) {
    rows = rows.slice(q._offsetValue)
  }

  // .limit()
  if (q._limitValue) {
    rows = rows.slice(0, Math.min(q._limitValue, rows.length))
  }

  return (rows as any) as OUT[]
}
