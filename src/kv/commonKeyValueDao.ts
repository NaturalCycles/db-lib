import { AppError, CommonLogger, KeyValueTuple, pMap } from '@naturalcycles/js-lib'
import { deflateString, inflateToString, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDaoLogLevel } from '../commondao/common.dao.model'
import { CommonDBCreateOptions } from '../db.model'
import {
  CommonKeyValueDB,
  CommonKeyValueDBSaveBatchOptions,
  KeyValueDBTuple,
} from './commonKeyValueDB'

export interface CommonKeyValueDaoCfg<V> {
  db: CommonKeyValueDB

  table: string

  /**
   * @default to false
   * Set to true to limit DB writing (will throw an error is such case).
   */
  readOnly?: boolean

  /**
   * Default to console
   */
  logger?: CommonLogger

  /**
   * @default OPERATIONS
   */
  logLevel?: CommonDaoLogLevel

  /**
   * @default false
   */
  logStarted?: boolean

  hooks?: {
    mapValueToBuffer?: (v: V) => Promise<Buffer>
    mapBufferToValue?: (b: Buffer) => Promise<V>
    beforeCreate?: (v: Partial<V>) => Partial<V>
  }

  /**
   * Set to `true` to conveniently enable zipping+JSON.stringify
   * (and unzipping+JSON.parse) of the Buffer value via hooks.
   * Custom hooks will override these hooks (if provided).
   */
  deflatedJsonValue?: boolean
}

export type CommonKeyValueDaoSaveOptions = CommonKeyValueDBSaveBatchOptions

// todo: logging
// todo: readonly

export class CommonKeyValueDao<V, K extends string = string> {
  constructor(cfg: CommonKeyValueDaoCfg<V>) {
    this.cfg = {
      hooks: {},
      logger: console,
      ...cfg,
    }

    if (cfg.deflatedJsonValue) {
      this.cfg.hooks = {
        mapValueToBuffer: async v => await deflateString(JSON.stringify(v)),
        mapBufferToValue: async buf => JSON.parse(await inflateToString(buf)),
        ...cfg.hooks,
      }
    }
  }

  cfg: CommonKeyValueDaoCfg<V> & {
    hooks: NonNullable<CommonKeyValueDaoCfg<V>['hooks']>
    logger: CommonLogger
  }

  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async createTable(opt: CommonDBCreateOptions = {}): Promise<void> {
    await this.cfg.db.createTable(this.cfg.table, opt)
  }

  create(input: Partial<V> = {}): V {
    return {
      ...this.cfg.hooks.beforeCreate?.(input),
    } as V
  }

  async getById(id?: K): Promise<V | null> {
    if (!id) return null
    const [r] = await this.getByIds([id])
    return r?.[1] || null
  }

  async getByIdAsBuffer(id?: K): Promise<Buffer | null> {
    if (!id) return null
    const [r] = await this.cfg.db.getByIds(this.cfg.table, [id])
    return r?.[1] || null
  }

  async requireById(id: K): Promise<V> {
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

  async requireByIdAsBuffer(id: K): Promise<Buffer> {
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

  async getByIdOrEmpty(id: K, part: Partial<V> = {}): Promise<V> {
    const [r] = await this.getByIds([id])
    if (r) return r[1]

    return {
      ...this.cfg.hooks.beforeCreate?.({}),
      ...part,
    } as V
  }

  async patch(id: K, patch: Partial<V>, opt?: CommonKeyValueDaoSaveOptions): Promise<V> {
    const v: V = {
      ...(await this.getByIdOrEmpty(id)),
      ...patch,
    }

    await this.save(id, v, opt)

    return v
  }

  async getByIds(ids: K[]): Promise<KeyValueTuple<string, V>[]> {
    const entries = await this.cfg.db.getByIds(this.cfg.table, ids)
    if (!this.cfg.hooks.mapBufferToValue) return entries as any

    return await pMap(entries, async ([id, buf]) => [
      id,
      await this.cfg.hooks.mapBufferToValue!(buf),
    ])
  }

  async getByIdsAsBuffer(ids: K[]): Promise<KeyValueTuple<K, Buffer>[]> {
    return (await this.cfg.db.getByIds(this.cfg.table, ids)) as KeyValueTuple<K, Buffer>[]
  }

  async save(id: K, value: V, opt?: CommonKeyValueDaoSaveOptions): Promise<void> {
    await this.saveBatch([[id, value]], opt)
  }

  async saveAsBuffer(id: K, value: Buffer, opt?: CommonKeyValueDaoSaveOptions): Promise<void> {
    await this.cfg.db.saveBatch(this.cfg.table, [[id, value]], opt)
  }

  async saveBatch(
    entries: KeyValueTuple<K, V>[],
    opt?: CommonKeyValueDaoSaveOptions,
  ): Promise<void> {
    const { mapValueToBuffer } = this.cfg.hooks
    let bufferEntries: KeyValueDBTuple[]

    if (!mapValueToBuffer) {
      bufferEntries = entries as any
    } else {
      bufferEntries = await pMap(entries, async ([id, v]) => [id, await mapValueToBuffer(v)])
    }

    await this.cfg.db.saveBatch(this.cfg.table, bufferEntries, opt)
  }

  async saveBatchAsBuffer(
    entries: KeyValueDBTuple[],
    opt?: CommonKeyValueDaoSaveOptions,
  ): Promise<void> {
    await this.cfg.db.saveBatch(this.cfg.table, entries, opt)
  }

  async deleteByIds(ids: K[]): Promise<void> {
    await this.cfg.db.deleteByIds(this.cfg.table, ids)
  }

  async deleteById(id: K): Promise<void> {
    await this.cfg.db.deleteByIds(this.cfg.table, [id])
  }

  streamIds(limit?: number): ReadableTyped<K> {
    return this.cfg.db.streamIds(this.cfg.table, limit) as ReadableTyped<K>
  }

  streamValues(limit?: number): ReadableTyped<V> {
    const { mapBufferToValue } = this.cfg.hooks

    if (!mapBufferToValue) {
      return this.cfg.db.streamValues(this.cfg.table, limit) as ReadableTyped<V>
    }

    return this.cfg.db.streamValues(this.cfg.table, limit).flatMap(
      async buf => {
        try {
          return [await mapBufferToValue(buf)]
        } catch (err) {
          this.cfg.logger.error(err)
          return [] // SKIP
        }
      },
      {
        concurrency: 32,
      },
    )
  }

  streamEntries(limit?: number): ReadableTyped<KeyValueTuple<K, V>> {
    const { mapBufferToValue } = this.cfg.hooks

    if (!mapBufferToValue) {
      return this.cfg.db.streamEntries(this.cfg.table, limit) as ReadableTyped<KeyValueTuple<K, V>>
    }

    return (
      this.cfg.db.streamEntries(this.cfg.table, limit) as ReadableTyped<KeyValueTuple<K, Buffer>>
    ).flatMap(
      async ([id, buf]) => {
        try {
          return [[id, await mapBufferToValue(buf)]]
        } catch (err) {
          this.cfg.logger.error(err)
          return [] // SKIP
        }
      },
      {
        concurrency: 32,
      },
    )
  }

  async getAllKeys(limit?: number): Promise<K[]> {
    return await this.streamIds(limit).toArray()
  }

  async getAllValues(limit?: number): Promise<V[]> {
    return await this.streamValues(limit).toArray()
  }

  async getAllEntries(limit?: number): Promise<KeyValueTuple<K, V>[]> {
    return await this.streamEntries(limit).toArray()
  }

  /**
   * Increments the `id` field by the amount specified in `by`,
   * or by 1 if `by` is not specified.
   *
   * Returns the new value of the field.
   */
  async increment(id: K, by = 1): Promise<number> {
    return await this.cfg.db.increment(this.cfg.table, id, by)
  }
}
