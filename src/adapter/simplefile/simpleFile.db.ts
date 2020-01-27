import { by, sortObjectDeep } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import * as fs from 'fs-extra'
import { Readable } from 'stream'
import { CommonDB } from '../../common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  RunQueryResult,
  SavedDBEntity,
} from '../../db.model'
import { DBQuery } from '../../dbQuery'
import { CommonSchema } from '../../schema/common.schema'
import { CommonSchemaGenerator } from '../../schema/commonSchemaGenerator'
import { queryInMemory } from '../inmemory/inMemory.db'

export interface SimpleFileDBCfg {
  /**
   * Absolute path, please.
   * @default to `${process.cwd()}/tmp/storage`
   */
  storageDir?: string

  /**
   * @default true
   */
  prettyJson?: boolean

  /**
   * @default false
   * If true - will run `sortObjectDeep()` on each object to achieve deterministic sort
   */
  sortObjects?: boolean

  /**
   * @default false
   *
   * If true - writes target files in newling-delimited json format
   */
  ndjson?: boolean

  /**
   * @default `json`
   * @default `ndjson` if ndjson=true
   */
  ext?: string
}

// const log = Debug('nc:db-lib:file')

export class SimpleFileDB implements CommonDB {
  constructor(cfg: SimpleFileDBCfg = {}) {
    this.cfg = {
      storageDir: `${process.cwd()}/tmp/storage`,
      ndjson: false,
      ext: cfg.ndjson ? 'ndjson' : 'json',
      ...cfg,
      prettyJson: cfg.prettyJson !== false, // default true
      sortObjects: cfg.sortObjects !== false, // default true
    }

    fs.ensureDirSync(this.cfg.storageDir)
  }

  cfg!: Required<SimpleFileDBCfg>

  cache: Record<string, Record<string, any>> = {}

  async resetCache(table?: string): Promise<void> {
    if (table) {
      await this.saveTable(table, {})
    } else {
      await fs.emptyDir(this.cfg.storageDir)
      this.cache = {}
    }
  }

  private async getTable<DBM extends SavedDBEntity>(table: string): Promise<Record<string, DBM>> {
    if (!this.cache[table]) {
      if (this.cfg.ndjson) {
        const data = await fs
          .readFile(`${this.cfg.storageDir}/${table}.${this.cfg.ext}`, 'utf8')
          .catch(() => '')

        const rows = data
          .split('\n')
          .filter(Boolean)
          .map(row => JSON.parse(row) as DBM)
        this.cache[table] = by(rows, r => r.id)
      } else {
        this.cache[table] = await fs
          .readJson(`${this.cfg.storageDir}/${table}.${this.cfg.ext}`)
          .catch(() => ({}))
      }
    }
    return this.cache[table]
  }

  private async saveTable<DBM extends SavedDBEntity>(
    table: string,
    data: Record<string, DBM>,
  ): Promise<void> {
    if (this.cfg.sortObjects) {
      data = sortObjectDeep(data)
    }

    this.cache[table] = data
    const filePath = `${this.cfg.storageDir}/${table}.${this.cfg.ext}`
    await fs.ensureFile(filePath)

    if (this.cfg.ndjson) {
      await fs.writeFile(
        filePath,
        Object.values(data)
          .map(r => JSON.stringify(r))
          .join('\n'),
      )
    } else {
      await fs.writeJson(filePath, data, this.cfg.prettyJson ? { spaces: 2 } : {})
    }
  }

  async getTables(): Promise<string[]> {
    // scan all files (w/o subfolders) in dir, ending with ext
    return (await fs.readdir(this.cfg.storageDir))
      .filter(f => f.endsWith(this.cfg.ext))
      .map(f => f.substr(0, f.length - this.cfg.ext.length - 1)) // -1 is to include '.'
  }

  async getTableSchema<DBM extends SavedDBEntity>(table: string): Promise<CommonSchema<DBM>> {
    const rows = Object.values(await this.getTable<DBM>(table))
    return CommonSchemaGenerator.generateFromRows<DBM>({ table }, rows)
  }

  async createTable(schema: CommonSchema, opt: CommonDBCreateOptions = {}): Promise<void> {
    if (opt.dropIfExists || !this.cache[schema.table]) {
      await this.saveTable(schema.table, {})
    }
  }

  async getByIds<DBM extends SavedDBEntity>(
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    const data = await this.getTable<DBM>(table)
    return ids.map(id => data[id]).filter(Boolean)
  }

  async deleteByIds(table: string, ids: string[], opts?: CommonDBOptions): Promise<number> {
    const data = await this.getTable(table)
    const deletedIds: string[] = []
    ids.forEach(id => {
      if (data[id]) {
        deletedIds.push(id)
        delete data[id]
      }
    })
    await this.saveTable(table, data)

    return deletedIds.length
  }

  async saveBatch<DBM extends SavedDBEntity>(
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void> {
    const data = await this.getTable(table)
    dbms.forEach(dbm => {
      data[dbm.id] = dbm
    })
    await this.saveTable(table, data)
  }

  async runQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opts?: CommonDBOptions,
  ): Promise<RunQueryResult<OUT>> {
    return { records: queryInMemory<DBM, OUT>(q, Object.values(await this.getTable(q.table))) }
  }

  async runQueryCount(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    const rows = queryInMemory(q, Object.values(await this.getTable(q.table)))
    return rows.length
  }

  streamQuery<DBM extends SavedDBEntity, OUT = DBM>(
    q: DBQuery<any, DBM>,
    opts?: CommonDBOptions,
  ): ReadableTyped<OUT> {
    const readable = new Readable({
      objectMode: true,
      read() {},
    })

    void this.getTable<DBM>(q.table).then(data => {
      queryInMemory<DBM, OUT>(q, Object.values(data)).forEach(dbm => readable.push(dbm))
      readable.push(null) // "complete" the stream
    })

    return readable
  }

  async deleteByQuery(q: DBQuery, opts?: CommonDBOptions): Promise<number> {
    const data = await this.getTable(q.table)
    const rows = queryInMemory(q, Object.values(data))
    const deletedIds = rows.map(dbm => dbm.id)
    deletedIds.forEach(id => delete data[id])
    await this.saveTable(q.table, data)
    return deletedIds.length
  }
}
