import { Debug, IDebugger, readableFrom } from '@naturalcycles/nodejs-lib'
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
    opt: CommonDBOptions = {},
  ): Promise<DBM[]> {
    const resultMap: Record<string, DBM> = {}
    const missingIds: string[] = []

    if (!opt.skipCache && !this.cfg.skipCache) {
      const results = await this.cfg.cacheDB.getByIds<DBM>(table, ids, opt)

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

    if (missingIds.length && !opt.onlyCache && !this.cfg.onlyCache) {
      const results = await this.cfg.downstreamDB.getByIds<DBM>(table, missingIds, opt)
      results.forEach(r => (resultMap[r.id] = r))

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.getByIds ${results.length} rows from downstream: [${results
            .map(r => r.id)
            .join(', ')}]`,
        )
      }

      if (!opt.skipCache) {
        const cacheResult = this.cfg.cacheDB.saveBatch(table, results, opt)
        if (this.cfg.awaitCache) await cacheResult
      }
    }

    // return in right order
    return ids.map(id => resultMap[id]).filter(Boolean)
  }

  async deleteByIds(table: string, ids: string[], opt: CommonDBOptions = {}): Promise<number> {
    let deletedIds = 0

    if (!opt.onlyCache && !this.cfg.onlyCache) {
      deletedIds = await this.cfg.downstreamDB.deleteByIds(table, ids, opt)

      if (this.cfg.logDownstream) {
        this.log(`${table}.deleteByIds ${deletedIds} rows from downstream`)
      }
    }

    if (!opt.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.deleteByIds(table, ids, opt).then(deletedFromCache => {
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
    opt: CommonDBSaveOptions = {},
  ): Promise<void> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      await this.cfg.downstreamDB.saveBatch(table, dbms, opt)

      if (this.cfg.logDownstream) {
        this.log(
          `${table}.saveBatch ${dbms.length} rows to downstream: [${dbms
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (!opt.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.saveBatch(table, dbms, opt).then(() => {
        if (this.cfg.logCached) {
          this.log(
            `${table}.saveBatch ${dbms.length} rows to cache: [${dbms.map(r => r.id).join(', ')}]`,
          )
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opt: CommonDBOptions = {},
  ): Promise<RunQueryResult<OUT>> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const { records, ...queryResult } = await this.cfg.downstreamDB.runQuery<DBM, OUT>(q, opt)

      if (this.cfg.logDownstream) {
        this.log(`${q.table}.runQuery ${records.length} rows from downstream`)
      }

      // Don't save to cache if it was a projection query
      if (!opt.skipCache && !opt.skipCache && !q._selectedFieldNames) {
        const cacheResult = this.cfg.cacheDB.saveBatch(q.table, records as any)
        if (this.cfg.awaitCache) await cacheResult
      }
      return { records, ...queryResult }
    }

    if (opt.skipCache || this.cfg.skipCache) return { records: [] }

    const { records, ...queryResult } = await this.cfg.cacheDB.runQuery<DBM, OUT>(q, opt)

    if (this.cfg.logCached) {
      this.log(`${q.table}.runQuery ${records.length} rows from cache`)
    }

    return { records, ...queryResult }
  }

  async runQueryCount(q: DBQuery, opt: CommonDBOptions = {}): Promise<number> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      return this.cfg.downstreamDB.runQueryCount(q, opt)
    }

    const count = await this.cfg.cacheDB.runQueryCount(q, opt)

    if (this.cfg.logCached) {
      this.log(`${q.table}.runQueryCount ${count} rows from cache`)
    }

    return count
  }

  streamQuery<DBM extends SavedDBEntity>(
    q: DBQuery<any, DBM>,
    opt: CommonDBSaveOptions = {},
  ): NodeJS.ReadableStream {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const stream = this.cfg.downstreamDB.streamQuery<DBM>(q, opt)

      // Don't save to cache if it was a projection query
      if (!opt.skipCache && !this.cfg.skipCache && !q._selectedFieldNames) {
        // todo: rethink if we really should download WHOLE stream into memory in order to save it to cache
        // void obs
        //   .pipe(toArray())
        //   .toPromise()
        //   .then(async dbms => {
        //     await this.cfg.cacheDB.saveBatch(q.table, dbms as any)
        //   })
      }

      return stream
    }

    if (opt.skipCache || this.cfg.skipCache) return readableFrom([])

    const stream = this.cfg.cacheDB.streamQuery<DBM>(q, opt)

    // if (this.cfg.logCached) {
    //   let count = 0
    //
    //   void pMapStream(stream, async () => {
    //     count++
    //   }, {concurrency: 10})
    //     .then(length => {
    //       this.log(`${q.table}.streamQuery ${length} rows from cache`)
    //     })
    // }

    return stream
  }

  async deleteByQuery(q: DBQuery, opt: CommonDBOptions = {}): Promise<number> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteByQuery(q, opt)

      if (this.cfg.logDownstream) {
        this.log(`${q.table}.deleteByQuery ${deletedIds} rows from downstream and cache`)
      }

      if (!opt.skipCache && !this.cfg.skipCache) {
        const cacheResult = this.cfg.cacheDB.deleteByQuery(q, opt)
        if (this.cfg.awaitCache) await cacheResult
      }

      return deletedIds
    }

    if (opt.skipCache || this.cfg.skipCache) return 0

    const deletedIds = await this.cfg.cacheDB.deleteByQuery(q, opt)

    if (this.cfg.logCached) {
      this.log(`${q.table}.deleteByQuery ${deletedIds} rows from cache`)
    }

    return deletedIds
  }
}
