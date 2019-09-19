import { Debug, IDebugger } from '@naturalcycles/nodejs-lib'
import { EMPTY, Observable } from 'rxjs'
import { count, toArray } from 'rxjs/operators'
import { CommonDB } from './common.db'
import { CommonDBOptions, CommonDBSaveOptions, RunQueryResult, SavedDBEntity } from './db.model'
import { DBQuery } from './dbQuery'

export interface CacheDBCfg {
  name: string
  cacheDB: CommonDB
  downstreamDB: CommonDB

  /**
   * If true - cache SAVING operations will await to be completed,
   * otherwise will be executed async
   *
   * @default false
   */
  awaitCache?: boolean

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

/**
 * CommonDB implementation that proxies requests to downstream CommonDB
 * and does in-memory caching.
 *
 * Queries always hit downstream (unless `onlyCache` is passed)
 */
export class CacheDB implements CommonDB {
  constructor(public cfg: CacheDBCfg) {
    this.log = Debug(`nc:db-lib:${cfg.name}`)
  }

  log!: IDebugger

  /**
   * Resets InMemory DB data
   */
  async resetCache(table?: string): Promise<void> {
    this.log(`resetCache ${table || 'all'}`)
    await this.cfg.cacheDB.resetCache(table)
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts: CommonDBOptions = {},
  ): Promise<DBM[]> {
    const resultMap: Record<string, DBM> = {}
    const missingIds: string[] = []

    if (!opts.skipCache && !this.cfg.skipCache) {
      const results = await this.cfg.cacheDB.getByIds<DBM>(table, ids, opts)

      results.forEach(r => (resultMap[r.id] = r))

      missingIds.push(...ids.filter(id => !resultMap[id]))

      if (this.cfg.logCached) {
        this.log(
          `${table}.getByIds ${results.length} rows from cache: [${results
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (missingIds.length && !opts.onlyCache && !this.cfg.onlyCache) {
      const results = await this.cfg.downstreamDB.getByIds<DBM>(table, missingIds, opts)
      results.forEach(r => (resultMap[r.id] = r))

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.getByIds ${results.length} rows from downstream: [${results
            .map(r => r.id)
            .join(', ')}]`,
        )
      }

      if (!opts.skipCache) {
        const cacheResult = this.cfg.cacheDB.saveBatch(table, results, opts)
        if (this.cfg.awaitCache) await cacheResult
      }
    }

    // return in right order
    return ids.map(id => resultMap[id]).filter(Boolean)
  }

  async deleteByIds(table: string, ids: string[], opts: CommonDBOptions = {}): Promise<number> {
    let deletedIds = 0

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      deletedIds = await this.cfg.downstreamDB.deleteByIds(table, ids, opts)

      if (this.cfg.logDownstream) {
        this.log(`${table}.deleteByIds ${deletedIds} rows from downstream`)
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.deleteByIds(table, ids, opts).then(deletedFromCache => {
        if (this.cfg.logCached) {
          this.log(`${table}.deleteByIds ${deletedFromCache} rows from cache`)
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }

    return deletedIds
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts: CommonDBSaveOptions = {},
  ): Promise<void> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      await this.cfg.downstreamDB.saveBatch(table, dbms, opts)

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.saveBatch ${dbms.length} rows to downstream: [${dbms
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.saveBatch(table, dbms, opts).then(() => {
        if (this.cfg.logCached) {
          this.log(
            `${table}.saveBatch ${dbms.length} rows to cache: [${dbms.map(r => r.id).join(', ')}]`,
          )
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }
  }

  async runQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts: CommonDBOptions = {},
  ): Promise<RunQueryResult<DBM>> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const { records, ...queryResult } = await this.cfg.downstreamDB.runQuery(q, opts)

      if (this.cfg.logDownstream) {
        this.log(`${q.table}.runQuery ${records.length} rows from downstream`)
      }

      if (!opts.skipCache && !opts.skipCache) {
        const cacheResult = this.cfg.cacheDB.saveBatch(q.table, records)
        if (this.cfg.awaitCache) await cacheResult
      }
      return { records, ...queryResult }
    }

    if (opts.skipCache || this.cfg.skipCache) return { records: [] }

    const { records, ...queryResult } = await this.cfg.cacheDB.runQuery(q, opts)

    if (this.cfg.logCached) {
      this.log(`${q.table}.runQuery ${records.length} rows from cache`)
    }

    return { records, ...queryResult }
  }

  async runQueryCount<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts: CommonDBOptions = {},
  ): Promise<number> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      return this.cfg.downstreamDB.runQueryCount(q, opts)
    }

    const count = await this.cfg.cacheDB.runQueryCount(q, opts)

    if (this.cfg.logCached) {
      this.log(`${q.table}.runQueryCount ${count} rows from cache`)
    }

    return count
  }

  streamQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts: CommonDBSaveOptions = {},
  ): Observable<DBM> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const obs = this.cfg.downstreamDB.streamQuery(q, opts)

      if (!opts.skipCache && !this.cfg.skipCache) {
        void obs
          .pipe(toArray())
          .toPromise()
          .then(async dbms => {
            await this.cfg.cacheDB.saveBatch(q.table, dbms)
          })
      }

      return obs
    }

    if (opts.skipCache || this.cfg.skipCache) return EMPTY

    const obs = this.cfg.cacheDB.streamQuery(q, opts)

    if (this.cfg.logCached) {
      void obs
        .pipe(count())
        .toPromise()
        .then(length => {
          this.log(`${q.table}.streamQuery ${length} rows from cache`)
        })
    }

    return obs
  }

  async deleteByQuery<DBM extends SavedDBEntity>(
    q: DBQuery<DBM>,
    opts: CommonDBOptions = {},
  ): Promise<number> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteByQuery(q, opts)

      if (this.cfg.logDownstream) {
        this.log(`${q.table}.deleteByQuery ${deletedIds} rows from downstream and cache`)
      }

      if (!opts.skipCache && !this.cfg.skipCache) {
        const cacheResult = this.cfg.cacheDB.deleteByQuery(q, opts)
        if (this.cfg.awaitCache) await cacheResult
      }

      return deletedIds
    }

    if (opts.skipCache || this.cfg.skipCache) return 0

    const deletedIds = await this.cfg.cacheDB.deleteByQuery(q, opts)

    if (this.cfg.logCached) {
      this.log(`${q.table}.deleteByQuery ${deletedIds} rows from cache`)
    }

    return deletedIds
  }
}
