import { AppError, CommonLogger, KeyValueTuple, pMap } from '@naturalcycles/js-lib'
import { deflateString, inflateToString, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDaoLogLevel } from '../commondao/common.dao.model'
import { CommonDBCreateOptions } from '../db.model'
import {
  CommonKeyValueDB,
  CommonKeyValueDBSaveBatchOptions,
  IncrementTuple,
} from './commonKeyValueDB'

export interface CommonKeyValueDaoCfg<RAW_V, V = RAW_V> {
  db: CommonKeyValueDB<RAW_V>

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

  transformer?: CommonKeyValueDaoTransformer<V, RAW_V>
}

export type CommonKeyValueDaoSaveOptions = CommonKeyValueDBSaveBatchOptions

export interface CommonKeyValueDaoTransformer<V, RAW_V> {
  valueToRaw: (v: V) => Promise<RAW_V>
  rawToValue: (raw: RAW_V) => Promise<V>
}

export const commonKeyValueDaoDeflatedJsonTransformer: CommonKeyValueDaoTransformer<any, Buffer> = {
  valueToRaw: async v => await deflateString(JSON.stringify(v)),
  rawToValue: async raw => JSON.parse(await inflateToString(raw)),
}

// todo: logging
// todo: readonly

export class CommonKeyValueDao<K extends string, RAW_V, V = RAW_V> {
  constructor(cfg: CommonKeyValueDaoCfg<RAW_V, V>) {
    this.cfg = {
      logger: console,
      ...cfg,
    }
  }

  cfg: CommonKeyValueDaoCfg<RAW_V, V> & {
    logger: CommonLogger
  }

  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async createTable(opt: CommonDBCreateOptions = {}): Promise<void> {
    await this.cfg.db.createTable(this.cfg.table, opt)
  }

  async getById(id?: K): Promise<V | null> {
    if (!id) return null
    const [r] = await this.getByIds([id])
    return r?.[1] || null
  }

  async getByIdRaw(id?: K): Promise<RAW_V | null> {
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

  async requireByIdRaw(id: K): Promise<RAW_V> {
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

  async getByIds(ids: K[]): Promise<KeyValueTuple<string, V>[]> {
    const entries = await this.cfg.db.getByIds(this.cfg.table, ids)
    if (!this.cfg.transformer) return entries as any

    return await pMap(entries, async ([id, raw]) => [
      id,
      await this.cfg.transformer!.rawToValue(raw),
    ])
  }

  async getByIdsRaw(ids: K[]): Promise<KeyValueTuple<K, RAW_V>[]> {
    return (await this.cfg.db.getByIds(this.cfg.table, ids)) as KeyValueTuple<K, RAW_V>[]
  }

  async save(id: K, value: V, opt?: CommonKeyValueDaoSaveOptions): Promise<void> {
    await this.saveBatch([[id, value]], opt)
  }

  async saveRaw(id: K, value: RAW_V, opt?: CommonKeyValueDaoSaveOptions): Promise<void> {
    await this.cfg.db.saveBatch(this.cfg.table, [[id, value]], opt)
  }

  async saveBatch(
    entries: KeyValueTuple<K, V>[],
    opt?: CommonKeyValueDaoSaveOptions,
  ): Promise<void> {
    const { transformer } = this.cfg
    let rawEntries: KeyValueTuple<string, RAW_V>[]

    if (!transformer) {
      rawEntries = entries as any
    } else {
      rawEntries = await pMap(entries, async ([id, v]) => [id, await transformer.valueToRaw(v)])
    }

    await this.cfg.db.saveBatch(this.cfg.table, rawEntries, opt)
  }

  async saveBatchRaw(
    entries: KeyValueTuple<K, RAW_V>[],
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
    const { transformer } = this.cfg

    if (!transformer) {
      return this.cfg.db.streamValues(this.cfg.table, limit) as unknown as ReadableTyped<V>
    }

    return this.cfg.db.streamValues(this.cfg.table, limit).flatMap(
      async raw => {
        try {
          return [await transformer.rawToValue(raw)]
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
    const { transformer } = this.cfg

    if (!transformer) {
      return this.cfg.db.streamEntries(this.cfg.table, limit) as any
    }

    return (
      this.cfg.db.streamEntries(this.cfg.table, limit) as ReadableTyped<KeyValueTuple<K, RAW_V>>
    ).flatMap(
      async ([id, raw]) => {
        try {
          return [[id, await transformer.rawToValue(raw)]]
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
    const [t] = await this.cfg.db.incrementBatch(this.cfg.table, [[id, by]])
    return t![1]
  }

  async incrementBatch(entries: IncrementTuple[]): Promise<IncrementTuple[]> {
    return await this.cfg.db.incrementBatch(this.cfg.table, entries)
  }
}
