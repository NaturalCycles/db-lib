import { pMap, StringMap, _stringMapEntries } from '@naturalcycles/js-lib'
import { CommonDaoLogLevel } from '../commondao/common.dao.model'
import { CommonDBCreateOptions } from '../db.model'
import { CommonKeyValueDB } from './commonKeyValueDB'

export interface CommonKeyValueDaoCfg<T> {
  db: CommonKeyValueDB

  table: string

  /**
   * @default to false
   * Set to true to limit DB writing (will throw an error is such case).
   */
  readOnly?: boolean

  /**
   * @default OPERATIONS
   */
  logLevel?: CommonDaoLogLevel

  /**
   * @default false
   */
  logStarted?: boolean

  hooks?: {
    mapValueToBuffer(v: T): Promise<Buffer>
    mapBufferToValue(b: Buffer): Promise<T>
  }
}

// todo: logging
// todo: readonly

export class CommonKeyValueDao<T> {
  constructor(public cfg: CommonKeyValueDaoCfg<T>) {}

  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async createTable(opt: CommonDBCreateOptions = {}): Promise<void> {
    await this.cfg.db.createTable(this.cfg.table, opt)
  }

  async getById(id?: string): Promise<T | null> {
    if (!id) return null
    const [r] = await this.getByIds([id])
    return r || null
  }

  async getByIds(ids: string[]): Promise<T[]> {
    const results = await this.cfg.db.getByIds(this.cfg.table, ids)
    if (!this.cfg.hooks?.mapBufferToValue) return results as any

    return await pMap(results, async r => await this.cfg.hooks!.mapBufferToValue(r))
  }

  async save(id: string, b: T): Promise<void> {
    await this.saveBatch({ [id]: b })
  }

  async saveBatch(batch: StringMap<T>): Promise<void> {
    let map: StringMap<Buffer> = {}

    if (!this.cfg.hooks?.mapValueToBuffer) {
      map = batch as any
    } else {
      await pMap(_stringMapEntries(batch), async ([id, v]) => {
        map[id] = await this.cfg.hooks!.mapValueToBuffer(v)
      })
    }

    await this.cfg.db.saveBatch(this.cfg.table, map)
  }

  async deleteByIds(ids: string[]): Promise<void> {
    await this.cfg.db.deleteByIds(this.cfg.table, ids)
  }
}
