import { AppError, ErrorMode, KeyValueTuple, pMap } from '@naturalcycles/js-lib'
import {
  deflateString,
  inflateToString,
  ReadableTyped,
  transformMap,
} from '@naturalcycles/nodejs-lib'
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
    mapValueToBuffer?: (v: T) => Promise<Buffer>
    mapBufferToValue?: (b: Buffer) => Promise<T>
    beforeCreate?: (v: Partial<T>) => Partial<T>
  }

  /**
   * Set to `true` to conveniently enable zipping+JSON.stringify
   * (and unzipping+JSON.parse) of the Buffer value via hooks.
   * Custom hooks will override these hooks (if provided).
   */
  deflatedJsonValue?: boolean
}

// todo: logging
// todo: readonly

export class CommonKeyValueDao<T> {
  constructor(public cfg: CommonKeyValueDaoCfg<T>) {
    if (cfg.deflatedJsonValue) {
      cfg.hooks = {
        mapValueToBuffer: async v => await deflateString(JSON.stringify(v)),
        mapBufferToValue: async buf => JSON.parse(await inflateToString(buf)),
        ...cfg.hooks,
      }
    }
  }

  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async createTable(opt: CommonDBCreateOptions = {}): Promise<void> {
    await this.cfg.db.createTable(this.cfg.table, opt)
  }

  create(input: Partial<T> = {}): T {
    return {
      ...this.cfg.hooks?.beforeCreate?.(input),
    } as T
  }

  async getById(id?: string): Promise<T | null> {
    if (!id) return null
    const [r] = await this.getByIds([id])
    return r?.[1] || null
  }

  async getByIdAsBuffer(id?: string): Promise<Buffer | null> {
    if (!id) return null
    const [r] = await this.cfg.db.getByIds(this.cfg.table, [id])
    return r?.[1] || null
  }

  async requireById(id: string): Promise<T> {
    const [r] = await this.getByIds([id])

    if (!r) {
      const { table } = this.cfg
      throw new AppError(`DB row required, but not found in ${table}`, {
        table,
        id,
      })
    }

    return r[1]
  }

  async requireByIdAsBuffer(id: string): Promise<Buffer> {
    const [r] = await this.cfg.db.getByIds(this.cfg.table, [id])

    if (!r) {
      const { table } = this.cfg
      throw new AppError(`DB row required, but not found in ${table}`, {
        table,
        id,
      })
    }

    return r[1]
  }

  async getByIdOrEmpty(id: string, part: Partial<T> = {}): Promise<T> {
    const [r] = await this.getByIds([id])
    if (r) return r[1]

    return {
      ...this.cfg.hooks?.beforeCreate?.({}),
      ...part,
    } as T
  }

  async patch(id: string, patch: Partial<T>): Promise<T> {
    const v: T = {
      ...(await this.getByIdOrEmpty(id)),
      ...patch,
    }

    await this.save(id, v)

    return v
  }

  async getByIds(ids: string[]): Promise<KeyValueTuple<string, T>[]> {
    const entries = await this.cfg.db.getByIds(this.cfg.table, ids)
    if (!this.cfg.hooks?.mapBufferToValue) return entries as any

    return await pMap(entries, async ([id, buf]) => [
      id,
      await this.cfg.hooks!.mapBufferToValue!(buf),
    ])
  }

  async getByIdsAsBuffer(ids: string[]): Promise<KeyValueTuple<string, Buffer>[]> {
    return await this.cfg.db.getByIds(this.cfg.table, ids)
  }

  async save(id: string, value: T): Promise<void> {
    await this.saveBatch([[id, value]])
  }

  async saveAsBuffer(id: string, value: Buffer): Promise<void> {
    await this.cfg.db.saveBatch(this.cfg.table, [[id, value]])
  }

  async saveBatch(entries: KeyValueTuple<string, T>[]): Promise<void> {
    let bufferEntries: KeyValueDBTuple[] = []

    if (!this.cfg.hooks?.mapValueToBuffer) {
      bufferEntries = entries as any
    } else {
      bufferEntries = await pMap(entries, async ([id, v]) => [
        id,
        await this.cfg.hooks!.mapValueToBuffer!(v),
      ])
    }

    await this.cfg.db.saveBatch(this.cfg.table, bufferEntries)
  }

  async saveBatchAsBuffer(entries: KeyValueDBTuple[]): Promise<void> {
    await this.cfg.db.saveBatch(this.cfg.table, entries)
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

  streamValues(limit?: number): ReadableTyped<T> {
    if (!this.cfg.hooks?.mapBufferToValue) {
      return this.cfg.db.streamValues(this.cfg.table, limit)
    }

    // todo: consider it when readableMap supports `errorMode: SUPPRESS`
    // readableMap(this.cfg.db.streamValues(this.cfg.table, limit), async buf => await this.cfg.hooks!.mapBufferToValue(buf))
    const stream: ReadableTyped<T> = this.cfg.db
      .streamValues(this.cfg.table, limit)
      .on('error', err => stream.emit('error', err))
      .pipe(
        transformMap(async buf => await this.cfg.hooks!.mapBufferToValue!(buf), {
          errorMode: ErrorMode.SUPPRESS, // cause .pipe cannot propagate errors
        }),
      )

    return stream
  }

  streamEntries(limit?: number): ReadableTyped<KeyValueTuple<string, T>> {
    if (!this.cfg.hooks?.mapBufferToValue) {
      return this.cfg.db.streamEntries(this.cfg.table, limit)
    }

    const stream: ReadableTyped<KeyValueTuple<string, T>> = this.cfg.db
      .streamEntries(this.cfg.table, limit)
      .on('error', err => stream.emit('error', err))
      .pipe(
        transformMap(async ([id, buf]) => [id, await this.cfg.hooks!.mapBufferToValue!(buf)], {
          errorMode: ErrorMode.SUPPRESS, // cause .pipe cannot propagate errors
        }),
      )

    return stream
  }
}
