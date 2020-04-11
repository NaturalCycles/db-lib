import { _pick } from '@naturalcycles/js-lib'
import { SavedDBEntity } from '../../db.model'
import { DBQuery, DBQueryFilterOperator } from '../../dbQuery'

type FilterFn = (v: any, val: any) => boolean
const FILTER_FNS: Record<DBQueryFilterOperator, FilterFn> = {
  '=': (v, val) => v === val,
  '<': (v, val) => v < val,
  '<=': (v, val) => v <= val,
  '>': (v, val) => v > val,
  '>=': (v, val) => v >= val,
  in: (v, val) => ((val as any[]) || []).includes(v),
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
    rows = rows.map(row => _pick(row, q._selectedFieldNames as any[]))
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
