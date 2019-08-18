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

  /**
   * Global default.
   * @default false
   */
  skipCache?: boolean

  /**
   * Global default.
   * @default false
   */
  onlyCache?: boolean

  /**
   * @default false
   */
  logCached?: boolean

  /**
   * @default false
   */
  logDownstream?: boolean
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
    this.cache[table] = this.cache[table] || {}

    if (!opts.skipCache && !this.cfg.skipCache) {
      ids.forEach(id => {
        const r = this.cache[table][id]
        if (r) {
          resultMap[id] = r
        } else {
          missingIds.push(id)
        }
      })

      if (this.cfg.logCached) {
        log(
          `${table}.getByIds ${Object.keys(resultMap).length} rows from cache: [${Object.keys(
            resultMap,
          ).join(', ')}]`,
        )
      }
    }

    if (missingIds.length && !opts.onlyCache && !this.cfg.onlyCache) {
      const results = await this.cfg.downstreamDB.getByIds<ObjectWithId>(table, missingIds, opts)
      results.forEach(r => {
        resultMap[r.id] = r as any
        if (!opts.skipCache) {
          this.cache[table][r.id] = r
        }
      })

      if (this.cfg.logDownstream) {
        log(
          `${table}.getByIds ${results.length} rows from downstream: [${results
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
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
    this.cache[table] = this.cache[table] || {}

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      deletedIds.push(...(await this.cfg.downstreamDB.deleteByIds(table, ids, opts)))

      if (this.cfg.logDownstream) {
        log(
          `${table}.deleteByIds ${deletedIds.length} rows from downstream: [${deletedIds.join(
            ', ',
          )}]`,
        )
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      const deletedFromCache: string[] = []
      ids.forEach(id => {
        if (this.cache[table][id]) {
          deletedIds.push(id)
          deletedFromCache.push(id)
          delete this.cache[table][id]
        }
      })

      if (this.cfg.logCached) {
        log(
          `${table}.deleteByIds ${
            deletedFromCache.length
          } rows from cache: [${deletedFromCache.join(', ')}]`,
        )
      }
    }

    return deletedIds
  }

  async saveBatch<DBM extends BaseDBEntity = any> (
    table: string,
    dbms: DBM[],
    opts: CacheCommonDBSaveOptions = {},
  ): Promise<DBM[]> {
    this.cache[table] = this.cache[table] || {}
    let savedDBMs = dbms
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      savedDBMs = await this.cfg.downstreamDB.saveBatch(table, dbms, opts)

      if (this.cfg.logDownstream) {
        log(
          `${table}.saveBatch ${savedDBMs.length} rows to downstream: [${savedDBMs
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      dbms.forEach(dbm => {
        this.cache[table][dbm.id] = dbm
      })

      if (this.cfg.logCached) {
        log(
          `${table}.saveBatch ${savedDBMs.length} rows to cache: [${savedDBMs
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    return savedDBMs
  }

  async runQuery<DBM = any> (q: DBQuery<DBM>, opts: CacheCommonDBOptions = {}): Promise<DBM[]> {
    this.cache[q.table] = this.cache[q.table] || {}

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const dbms = await this.cfg.downstreamDB.runQuery(q, opts)

      if (this.cfg.logDownstream) {
        log(`${q.table}.runQuery ${dbms.length} rows from downstream`)
      }

      if (!opts.skipCache && !opts.skipCache) {
        dbms.forEach((dbm: any) => {
          this.cache[q.table][dbm.id] = dbm
        })
      }
      return dbms
    }

    if (opts.skipCache || this.cfg.skipCache) return []

    const dbms = queryInMemory(q, this.cache[q.table])

    if (this.cfg.logCached) {
      log(`${q.table}.runQuery ${dbms.length} rows from cache`)
    }

    return dbms
  }

  async runQueryCount<DBM = any> (
    q: DBQuery<DBM>,
    opts: CacheCommonDBOptions = {},
  ): Promise<number> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      return this.cfg.downstreamDB.runQueryCount(q, opts)
    }

    const rows = await this.runQuery(q, opts)

    if (this.cfg.logCached) {
      log(`${q.table}.runQueryCount ${rows.length} rows from cache`)
    }

    return rows.length
  }

  streamQuery<DBM = any> (q: DBQuery<DBM>, opts: CacheCommonDBSaveOptions = {}): Observable<DBM> {
    this.cache[q.table] = this.cache[q.table] || {}

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      return this.cfg.downstreamDB.streamQuery(q, opts).pipe(
        tap((dbm: any) => {
          if (!opts.skipCache && !this.cfg.skipCache) {
            this.cache[q.table][dbm.id] = dbm
          }
        }),
      )
    }

    if (opts.skipCache || this.cfg.skipCache) return EMPTY

    const rows = queryInMemory(q, this.cache[q.table])

    if (this.cfg.logCached) {
      log(`${q.table}.streamQuery ${rows.length} rows from cache`)
    }

    return of(...rows)
  }

  async deleteBy (
    table: string,
    by: string,
    value: any,
    limit?: number,
    opts: CacheCommonDBOptions = {},
  ): Promise<string[]> {
    this.cache[table] = this.cache[table] || {}

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteBy(table, by, value, limit, opts)

      if (this.cfg.logDownstream) {
        log(
          `${table}.deleteBy ${
            deletedIds.length
          } rows from downstream and cache: [${deletedIds.join(', ')}]`,
        )
      }

      if (!opts.skipCache && !this.cfg.skipCache) {
        deletedIds.forEach(id => {
          delete this.cache[table][id]
        })
      }

      return deletedIds
    }

    if (opts.skipCache || this.cfg.skipCache) return []

    const deletedIds = (await this.runQuery(new DBQuery(table).filter(by, '=', value), opts)).map(
      row => row.id,
    )

    if (this.cfg.logCached) {
      log(`${table}.deleteBy ${deletedIds.length} rows from cache: [${deletedIds.join(', ')}]`)
    }

    if (this.cache[table]) {
      deletedIds.forEach(id => {
        delete this.cache[table][id]
      })
    }

    return deletedIds
  }
}
