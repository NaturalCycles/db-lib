import { ErrorMode, KeyValueTuple, pMap } from '@naturalcycles/js-lib'
import { ReadableTyped, transformMap } from '@naturalcycles/nodejs-lib'
import { CommonDaoLogLevel } from '../commondao/common.dao.model'
import { CommonDBCreateOptions } from '../db.model'
import { CommonKeyValueDB, KeyValueDBTuple } from './commonKeyValueDB'

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
    return r?.[1] || null
  }

  async getByIds(ids: string[]): Promise<KeyValueTuple<string, T>[]> {
    const entries = await this.cfg.db.getByIds(this.cfg.table, ids)
    if (!this.cfg.hooks?.mapBufferToValue) return entries as any

    return await pMap(entries, async ([id, buf]) => [
      id,
      await this.cfg.hooks!.mapBufferToValue(buf),
    ])
  }

  async save(id: string, value: T): Promise<void> {
    await this.saveBatch([[id, value]])
  }

  async saveBatch(entries: KeyValueTuple<string, T>[]): Promise<void> {
    let bufferEntries: KeyValueDBTuple[] = []

    if (!this.cfg.hooks?.mapValueToBuffer) {
      bufferEntries = entries as any
    } else {
      bufferEntries = await pMap(entries, async ([id, v]) => [
        id,
        await this.cfg.hooks!.mapValueToBuffer(v),
      ])
    }

    await this.cfg.db.saveBatch(this.cfg.table, bufferEntries)
  }

  async deleteByIds(ids: string[]): Promise<void> {
    await this.cfg.db.deleteByIds(this.cfg.table, ids)
  }

  async deleteById(id: string): Promise<void> {
    await this.cfg.db.deleteByIds(this.cfg.table, [id])
  }

  streamIds(limit?: number): ReadableTyped<string> {
    return this.cfg.db.streamIds(this.cfg.table, limit)
  }

  streamValues(limit?: number): ReadableTyped<Buffer> {
    if (!this.cfg.hooks?.mapBufferToValue) {
      return this.cfg.db.streamValues(this.cfg.table, limit)
    }

    // todo: consider it when readableMap supports `errorMode: SUPPRESS`
    // readableMap(this.cfg.db.streamValues(this.cfg.table, limit), async buf => await this.cfg.hooks!.mapBufferToValue(buf))
    return this.cfg.db.streamValues(this.cfg.table, limit).pipe(
      transformMap(async buf => await this.cfg.hooks!.mapBufferToValue(buf), {
        errorMode: ErrorMode.SUPPRESS, // cause .pipe cannot propagate errors
      }),
    )
  }

  streamEntries(limit?: number): ReadableTyped<KeyValueTuple<string, T>> {
    if (!this.cfg.hooks?.mapBufferToValue) {
      return this.cfg.db.streamEntries(this.cfg.table, limit)
    }

    return this.cfg.db.streamEntries(this.cfg.table, limit).pipe(
      transformMap(async ([id, buf]) => [id, await this.cfg.hooks!.mapBufferToValue(buf)], {
        errorMode: ErrorMode.SUPPRESS, // cause .pipe cannot propagate errors
      }),
    )
  }
}
