import { StringMap } from '@naturalcycles/js-lib'
import { Debug } from '@naturalcycles/nodejs-lib'
import { EMPTY, Observable, of } from 'rxjs'
import { tap } from 'rxjs/operators'
import {
  BaseDBEntity,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  ObjectWithId,
} from './db.model'
import { DBQuery } from './dbQuery'
import { queryInMemory } from './inMemory.db'

export interface InMemoryCacheDBCfg {
  downstreamDB: CommonDB
}

export interface CacheCommonDBOptions extends CommonDBOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

export interface CacheCommonDBSaveOptions extends CommonDBSaveOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

const log = Debug('nc:db-lib:cache')

/**
 * CommonDB implementation that proxies requests to downstream CommonDB
 * and does in-memory caching.
 *
 * Queries always hit downstream (unless `onlyCache` is passed)
 */
export class InMemoryCacheDB implements CommonDB {
  constructor (public cfg: InMemoryCacheDBCfg) {}

  /**
   * First level: Table name
   * Second level: id => record
   */
  cache: StringMap<StringMap<any>> = {}

  /**
   * Resets InMemory DB data
   */
  resetCache (): void {
    log('resetCache')
    this.cache = {}
  }

  async getByIds<DBM = any> (
    table: string,
    ids: string[],
    opts: CacheCommonDBOptions = {},
  ): Promise<DBM[]> {
    const resultMap: StringMap<DBM> = {}
    const missingIds: string[] = []

    if (!opts.skipCache) {
      ids.forEach(id => {
        const r = (this.cache[table] || {})[id]
        if (r) {
          resultMap[id] = r
        } else {
          missingIds.push(id)
        }
      })
    }

    if (missingIds.length && !opts.onlyCache) {
      const results = await this.cfg.downstreamDB.getByIds<ObjectWithId>(table, ids, opts)
      results.forEach(r => {
        resultMap[r.id] = r as any
        if (!opts.skipCache) {
          this.cache[table][r.id] = r
        }
      })
    }

    // return in right order
    return ids.map(id => resultMap[id]).filter(Boolean)
  }

  async deleteByIds (
    table: string,
    ids: string[],
    opts: CacheCommonDBOptions = {},
  ): Promise<string[]> {
    const deletedIds: string[] = []

    if (!opts.onlyCache) {
      deletedIds.push(...(await this.cfg.downstreamDB.deleteByIds(table, ids, opts)))
    }

    if (!opts.skipCache) {
      ids.forEach(id => {
        if (this.cache[table][id]) {
          deletedIds.push(id)
        }
        delete this.cache[table][id]
      })
    }

    return deletedIds
  }

  async saveBatch<DBM extends BaseDBEntity = any> (
    table: string,
    dbms: DBM[],
    opts: CacheCommonDBSaveOptions = {},
  ): Promise<DBM[]> {
    let savedDBMs = dbms
    if (!opts.onlyCache) {
      savedDBMs = await this.cfg.downstreamDB.saveBatch(table, dbms, opts)
    }

    if (!opts.skipCache) {
      this.cache[table] = this.cache[table] || {}

      dbms.forEach(dbm => {
        this.cache[table][dbm.id] = dbm
      })
    }

    return savedDBMs
  }

  async runQuery<DBM = any> (q: DBQuery<DBM>, opts: CacheCommonDBOptions = {}): Promise<DBM[]> {
    if (!opts.onlyCache) {
      const dbms = await this.cfg.downstreamDB.runQuery(q, opts)

      if (!opts.skipCache) {
        dbms.forEach((dbm: any) => {
          this.cache[q.table][dbm.id] = dbm
        })
      }
      return dbms
    }

    if (opts.skipCache) return []

    return queryInMemory(q, this.cache[q.table])
  }

  async runQueryCount<DBM = any> (
    q: DBQuery<DBM>,
    opts: CacheCommonDBOptions = {},
  ): Promise<number> {
    if (!opts.onlyCache) {
      return this.cfg.downstreamDB.runQueryCount(q, opts)
    }

    const rows = await this.runQuery(q, opts)
    return rows.length
  }

  streamQuery<DBM = any> (q: DBQuery<DBM>, opts: CacheCommonDBSaveOptions = {}): Observable<DBM> {
    if (!opts.onlyCache) {
      return this.cfg.downstreamDB.streamQuery(q, opts).pipe(
        tap((dbm: any) => {
          if (!opts.skipCache) {
            this.cache[q.table][dbm.id] = dbm
          }
        }),
      )
    }

    if (opts.skipCache) return EMPTY

    return of(...queryInMemory(q, this.cache[q.table]))
  }

  async deleteBy (
    table: string,
    by: string,
    value: any,
    limit?: number,
    opts: CacheCommonDBOptions = {},
  ): Promise<string[]> {
    if (!opts.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteBy(table, by, value, limit, opts)

      if (!opts.skipCache) {
        deletedIds.forEach(id => {
          delete this.cache[table][id]
        })
      }

      return deletedIds
    }

    if (opts.skipCache) return []

    const deletedIds = (await this.runQuery(new DBQuery(table).filter(by, '=', value), opts)).map(
      row => row.id,
    )

    if (this.cache[table]) {
      deletedIds.forEach(id => {
        delete this.cache[table][id]
      })
    }

    return deletedIds
  }
}
