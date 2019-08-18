import { StringMap } from '@naturalcycles/js-lib'
import * as fs from 'fs-extra'
import { Observable, Subject } from 'rxjs'
import { BaseDBEntity, CommonDB, CommonDBOptions, CommonDBSaveOptions } from './db.model'
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
  constructor (cfg: SimpleFileDBCfg) {
    this.cfg = {
      prettyJson: true,
      ...cfg,
    }

    fs.ensureDirSync(cfg.storageDir)
  }

  cfg!: Required<SimpleFileDBCfg>

  cache: StringMap<StringMap<any>> = {}

  async resetCache (): Promise<void> {
    await fs.emptyDir(this.cfg.storageDir)
    this.cache = {}
  }

  private async getTable<DBM extends BaseDBEntity> (table: string): Promise<StringMap<DBM>> {
    if (!this.cache[table]) {
      this.cache[table] = await fs
        .readJson(`${this.cfg.storageDir}/${table}.json`)
        .catch(() => ({}))
    }
    return this.cache[table]
  }

  private async saveTable<DBM extends BaseDBEntity> (
    table: string,
    data: StringMap<DBM>,
  ): Promise<void> {
    this.cache[table] = data
    const filePath = `${this.cfg.storageDir}/${table}.json`
    await fs.ensureFile(filePath)
    await fs.writeJson(filePath, data, this.cfg.prettyJson ? { spaces: 2 } : {})
  }

  async getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    const data = await this.getTable<DBM>(table)
    return ids.map(id => data[id]).filter(Boolean)
  }

  async deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]> {
    const data = await this.getTable(table)
    const deletedIds: string[] = []
    ids.forEach(id => {
      if (data[id]) {
        deletedIds.push(id)
        delete data[id]
      }
    })
    await this.saveTable(table, data)

    return deletedIds
  }

  async saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<DBM[]> {
    const data = await this.getTable(table)
    dbms.forEach(dbm => {
      data[dbm.id] = dbm
    })
    await this.saveTable(table, data)
    return [...dbms]
  }

  async runQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<DBM[]> {
    return queryInMemory(q, await this.getTable(q.table))
  }

  async runQueryCount<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<number> {
    const rows = queryInMemory(q, await this.getTable(q.table))
    return rows.length
  }

  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM> {
    const subj = new Subject<DBM>()

    void this.getTable<DBM>(q.table).then(data => {
      queryInMemory(q, data).forEach(dbm => subj.next(dbm))
      subj.complete()
    })

    return subj
  }

  async deleteByQuery<DBM extends BaseDBEntity> (
    q: DBQuery<DBM>,
    opts?: CommonDBOptions,
  ): Promise<string[]> {
    const data = await this.getTable(q.table)
    const rows = queryInMemory(q, data)
    const deletedIds = rows.map(dbm => dbm.id)
    deletedIds.forEach(id => delete data[id])
    await this.saveTable(q.table, data)
    return deletedIds
  }
}
