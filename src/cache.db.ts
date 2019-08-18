import { StringMap } from '@naturalcycles/js-lib'
import { Debug, IDebugger } from '@naturalcycles/nodejs-lib'
import { EMPTY, Observable } from 'rxjs'
import { toArray } from 'rxjs/operators'
import { BaseDBEntity, CommonDB, CommonDBOptions, CommonDBSaveOptions } from './db.model'
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
  constructor (public cfg: CacheDBCfg) {
    this.log = Debug(`nc:db-lib:${cfg.name}`)
  }

  log!: IDebugger

  /**
   * Resets InMemory DB data
   */
  async resetCache (): Promise<void> {
    this.log('resetCache')
    await this.cfg.cacheDB.resetCache()
  }

  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts: CommonDBOptions = {},
  ): Promise<DBM[]> {
    const resultMap: StringMap<DBM> = {}
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

  async deleteByIds (table: string, ids: string[], opts: CommonDBOptions = {}): Promise<string[]> {
    const deletedIds: string[] = []

    if (!opts.onlyCache && !this.cfg.onlyCache) {
      deletedIds.push(...(await this.cfg.downstreamDB.deleteByIds(table, ids, opts)))

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.deleteByIds ${deletedIds.length} rows from downstream: [${deletedIds.join(
            ', ',
          )}]`,
        )
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.deleteByIds(table, ids, opts).then(deletedFromCache => {
        if (this.cfg.logCached) {
          this.log(
            `${table}.deleteByIds ${
              deletedFromCache.length
            } rows from cache: [${deletedFromCache.join(', ')}]`,
          )
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }

    return deletedIds
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts: CommonDBSaveOptions = {},
  ): Promise<DBM[]> {
    let savedDBMs = dbms
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      savedDBMs = await this.cfg.downstreamDB.saveBatch(table, dbms, opts)

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.saveBatch ${savedDBMs.length} rows to downstream: [${savedDBMs
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (!opts.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.saveBatch(table, dbms, opts).then(() => {
        if (this.cfg.logCached) {
          this.log(
            `${table}.saveBatch ${savedDBMs.length} rows to cache: [${savedDBMs
              .map(r => r.id)
              .join(', ')}]`,
          )
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }

    return savedDBMs
  }

  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts: CommonDBOptions = {},
  ): Promise<DBM[]> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const dbms = await this.cfg.downstreamDB.runQuery(q, opts)

      if (this.cfg.logDownstream) {
        this.log(`${q.table}.runQuery ${dbms.length} rows from downstream`)
      }

      if (!opts.skipCache && !opts.skipCache) {
        const cacheResult = this.cfg.cacheDB.saveBatch(q.table, dbms)
        if (this.cfg.awaitCache) await cacheResult
      }
      return dbms
    }

    if (opts.skipCache || this.cfg.skipCache) return []

    const dbms = await this.cfg.cacheDB.runQuery(q, opts)

    if (this.cfg.logCached) {
      this.log(`${q.table}.runQuery ${dbms.length} rows from cache`)
    }

    return dbms
  }

  async runQueryCount<DBM extends BaseDBEntity> (
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

  streamQuery<DBM extends BaseDBEntity> (
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
        .pipe(toArray())
        .toPromise()
        .then(dbms => {
          this.log(`${q.table}.streamQuery ${dbms.length} rows from cache`)
        })
    }

    return obs
  }

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts: CommonDBOptions = {},
  ): Promise<string[]> {
    if (!opts.onlyCache && !this.cfg.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteByQuery(q, opts)

      if (this.cfg.logDownstream) {
        this.log(
          `${q.table}.deleteByQuery ${
            deletedIds.length
          } rows from downstream and cache: [${deletedIds.join(', ')}]`,
        )
      }

      if (!opts.skipCache && !this.cfg.skipCache) {
        const cacheResult = this.cfg.cacheDB.deleteByQuery(q, opts)
        if (this.cfg.awaitCache) await cacheResult
      }

      return deletedIds
    }

    if (opts.skipCache || this.cfg.skipCache) return []

    const deletedIds = await this.cfg.cacheDB.deleteByQuery(q, opts)

    if (this.cfg.logCached) {
      this.log(
        `${q.table}.deleteByQuery ${deletedIds.length} rows from cache: [${deletedIds.join(', ')}]`,
      )
    }

    return deletedIds
  }
}
