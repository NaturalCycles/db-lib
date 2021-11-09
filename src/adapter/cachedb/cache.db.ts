import { Readable } from 'stream'
import {
  JsonSchemaObject,
  JsonSchemaRootObject,
  ObjectWithId,
  StringMap,
} from '@naturalcycles/js-lib'
import { BaseCommonDB } from '../../base.common.db'
import { CommonDB } from '../../common.db'
import { RunQueryResult } from '../../db.model'
import { DBQuery } from '../../query/dbQuery'
import {
  CacheDBCfg,
  CacheDBCreateOptions,
  CacheDBOptions,
  CacheDBStreamOptions,
} from './cache.db.model'

const isGAE = !!process.env['GAE_INSTANCE']

/**
 * CommonDB implementation that proxies requests to downstream CommonDB
 * and does in-memory caching.
 *
 * Queries always hit downstream (unless `onlyCache` is passed)
 */
export class CacheDB extends BaseCommonDB implements CommonDB {
  constructor(cfg: CacheDBCfg) {
    super()
    this.cfg = {
      logger: isGAE ? undefined : console,
      ...cfg,
    }
  }

  cfg: CacheDBCfg

  override async ping(): Promise<void> {
    await Promise.all([this.cfg.cacheDB.ping(), this.cfg.downstreamDB.ping()])
  }

  /**
   * Resets InMemory DB data
   */
  // This method is no longer in the public API. Call it just on the InMemoryDB if needed.
  // async resetCache(table?: string): Promise<void> {
  //   this.log(`resetCache ${table || 'all'}`)
  //   await this.cfg.cacheDB.resetCache(table)
  // }

  override async getTables(): Promise<string[]> {
    return await this.cfg.downstreamDB.getTables()
  }

  override async getTableSchema<ROW extends ObjectWithId>(
    table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    return await this.cfg.downstreamDB.getTableSchema<ROW>(table)
  }

  override async createTable<ROW extends ObjectWithId>(
    table: string,
    schema: JsonSchemaObject<ROW>,
    opt: CacheDBCreateOptions<ROW> = {},
  ): Promise<void> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      await this.cfg.downstreamDB.createTable(table, schema, opt)
    }

    if (!opt.skipCache && !this.cfg.skipCache) {
      await this.cfg.cacheDB.createTable(table, schema, opt)
    }
  }

  override async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt: CacheDBOptions<ROW> = {},
  ): Promise<ROW[]> {
    const resultMap: StringMap<ROW> = {}
    const missingIds: string[] = []

    if (!opt.skipCache && !this.cfg.skipCache) {
      const results = await this.cfg.cacheDB.getByIds<ROW>(table, ids, opt)

      results.forEach(r => (resultMap[r.id] = r))

      missingIds.push(...ids.filter(id => !resultMap[id]))

      if (this.cfg.logCached) {
        this.cfg.logger?.log(
          `${table}.getByIds ${results.length} rows from cache: [${results
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (missingIds.length && !opt.onlyCache && !this.cfg.onlyCache) {
      const results = await this.cfg.downstreamDB.getByIds<ROW>(table, missingIds, opt)
      results.forEach(r => (resultMap[r.id] = r))

      if (this.cfg.logDownstream) {
        this.cfg.logger?.log(
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
    return ids.map(id => resultMap[id]!).filter(Boolean)
  }

  override async deleteByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt: CacheDBOptions<ROW> = {},
  ): Promise<number> {
    let deletedIds = 0

    if (!opt.onlyCache && !this.cfg.onlyCache) {
      deletedIds = await this.cfg.downstreamDB.deleteByIds(table, ids, opt)

      if (this.cfg.logDownstream) {
        this.cfg.logger?.log(`${table}.deleteByIds ${deletedIds} rows from downstream`)
      }
    }

    if (!opt.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.deleteByIds(table, ids, opt).then(deletedFromCache => {
        if (this.cfg.logCached) {
          this.cfg.logger?.log(`${table}.deleteByIds ${deletedFromCache} rows from cache`)
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }

    return deletedIds
  }

  override async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt: CacheDBOptions<ROW> = {},
  ): Promise<void> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      await this.cfg.downstreamDB.saveBatch(table, rows, opt)

      if (this.cfg.logDownstream) {
        this.cfg.logger?.log(
          `${table}.saveBatch ${rows.length} rows to downstream: [${rows
            .map(r => r.id)
            .join(', ')}]`,
        )
      }
    }

    if (!opt.skipCache && !this.cfg.skipCache) {
      const cacheResult = this.cfg.cacheDB.saveBatch(table, rows, opt).then(() => {
        if (this.cfg.logCached) {
          this.cfg.logger?.log(
            `${table}.saveBatch ${rows.length} rows to cache: [${rows.map(r => r.id).join(', ')}]`,
          )
        }
      })
      if (this.cfg.awaitCache) await cacheResult
    }
  }

  override async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt: CacheDBOptions<ROW> = {},
  ): Promise<RunQueryResult<ROW>> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const { rows, ...queryResult } = await this.cfg.downstreamDB.runQuery(q, opt)

      if (this.cfg.logDownstream) {
        this.cfg.logger?.log(`${q.table}.runQuery ${rows.length} rows from downstream`)
      }

      // Don't save to cache if it was a projection query
      if (!opt.skipCache && !opt.skipCache && !q._selectedFieldNames) {
        const cacheResult = this.cfg.cacheDB.saveBatch(q.table, rows as any, opt)
        if (this.cfg.awaitCache) await cacheResult
      }
      return { rows, ...queryResult }
    }

    if (opt.skipCache || this.cfg.skipCache) return { rows: [] }

    const { rows, ...queryResult } = await this.cfg.cacheDB.runQuery(q, opt)

    if (this.cfg.logCached) {
      this.cfg.logger?.log(`${q.table}.runQuery ${rows.length} rows from cache`)
    }

    return { rows, ...queryResult }
  }

  override async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt: CacheDBOptions<ROW> = {},
  ): Promise<number> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      return await this.cfg.downstreamDB.runQueryCount(q, opt)
    }

    const count = await this.cfg.cacheDB.runQueryCount(q, opt)

    if (this.cfg.logCached) {
      this.cfg.logger?.log(`${q.table}.runQueryCount ${count} rows from cache`)
    }

    return count
  }

  override streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt: CacheDBStreamOptions<ROW> = {},
  ): Readable {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const stream = this.cfg.downstreamDB.streamQuery<ROW>(q, opt)

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

    if (opt.skipCache || this.cfg.skipCache) return Readable.from([])

    const stream = this.cfg.cacheDB.streamQuery<ROW>(q, opt)

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

  override async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt: CacheDBOptions<ROW> = {},
  ): Promise<number> {
    if (!opt.onlyCache && !this.cfg.onlyCache) {
      const deletedIds = await this.cfg.downstreamDB.deleteByQuery(q, opt)

      if (this.cfg.logDownstream) {
        this.cfg.logger?.log(
          `${q.table}.deleteByQuery ${deletedIds} rows from downstream and cache`,
        )
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
      this.cfg.logger?.log(`${q.table}.deleteByQuery ${deletedIds} rows from cache`)
    }

    return deletedIds
  }
}
