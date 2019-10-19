import * as fs from 'fs-extra'
import { Readable } from 'stream'
import { CommonDB } from './common.db'
import { CommonDBOptions, CommonDBSaveOptions, RunQueryResult, SavedDBEntity } from './db.model'
import { DBQuery } from './dbQuery'
import { queryInMemory } from './inMemory.db'

export interface SimpleFileDBCfg {
  /**
   * Absolute path, please.
   */
  storageDir: string

  /**
   * @default true
   */
  prettyJson?: boolean
}

// const log = Debug('nc:db-lib:file')

export class SimpleFileDB implements CommonDB {
  constructor(cfg: SimpleFileDBCfg) {
    this.cfg = {
      prettyJson: true,
      ...cfg,
    }

    fs.ensureDirSync(cfg.storageDir)
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
      this.cache[table] = await fs
        .readJson(`${this.cfg.storageDir}/${table}.json`)
        .catch(() => ({}))
    }
    return this.cache[table]
  }

  private async saveTable<DBM extends SavedDBEntity>(
    table: string,
    data: Record<string, DBM>,
  ): Promise<void> {
    this.cache[table] = data
    const filePath = `${this.cfg.storageDir}/${table}.json`
    await fs.ensureFile(filePath)
    await fs.writeJson(filePath, data, this.cfg.prettyJson ? { spaces: 2 } : {})
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

  streamQuery<DBM extends SavedDBEntity>(q: DBQuery<any, DBM>, opts?: CommonDBOptions): Readable {
    const readable = new Readable({
      objectMode: true,
      read() {},
    })

    void this.getTable<DBM>(q.table).then(data => {
      queryInMemory<DBM>(q, Object.values(data)).forEach(dbm => readable.push(dbm))
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
