import { StringMap, UnixTimestampNumber } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDBCreateOptions } from '../db.model'

/**
 * Common interface for Key-Value database implementations.
 *
 * @experimental
 */
export interface CommonKeyValueDB {
  /**
   * Manifest of supported features.
   */
  support: CommonKeyValueDBSupport

  /**
   * Check that DB connection is working properly.
   */
  ping: () => Promise<void>

  /**
   * Will do like `create table ...` for mysql.
   * Caution! dropIfExists defaults to false. If set to true - will actually DROP the table!
   */
  createTable: (table: string, opt?: CommonDBCreateOptions) => Promise<void>

  /**
   * Returns an array of tuples [key, value]. Not found values are not returned (no error is thrown).
   *
   * Currently it is NOT required to maintain the same order as input `ids`.
   */
  getByIds: (table: string, ids: string[]) => Promise<KeyValueDBTuple[]>

  deleteByIds: (table: string, ids: string[]) => Promise<void>

  saveBatch: (
    table: string,
    entries: KeyValueDBTuple[],
    opt?: CommonKeyValueDBSaveBatchOptions,
  ) => Promise<void>

  streamIds: (table: string, limit?: number) => ReadableTyped<string>
  streamValues: (table: string, limit?: number) => ReadableTyped<Buffer>
  streamEntries: (table: string, limit?: number) => ReadableTyped<KeyValueDBTuple>

  count: (table: string) => Promise<number>

  /**
   * Increments the value of a key in a table by a given amount.
   * Default increment is 1 when `by` is not provided.
   *
   * Returns the new value.
   *
   * @experimental
   */
  increment: (table: string, id: string, by?: number) => Promise<number>

  /**
   * Perform a batch of Increment operations.
   * Given incrementMap, increment each key of it by the given amount (value of the map).
   *
   * Example:
   * { key1: 2, key2: 3 }
   * would increment `key1` by 2, and `key2` by 3.
   *
   * Returns the incrementMap with the same keys and updated values.
   *
   * @experimental
   */
  incrementBatch: (table: string, incrementMap: StringMap<number>) => Promise<StringMap<number>>
}

export type KeyValueDBTuple = [key: string, value: Buffer]

export interface CommonKeyValueDBSaveBatchOptions {
  /**
   * If set (and if it's implemented by the driver) - will set expiry TTL for each key of the batch.
   * E.g EXAT in Redis.
   */
  expireAt?: UnixTimestampNumber
}

/**
 * Manifest of supported features.
 */
export interface CommonKeyValueDBSupport {
  count?: boolean
  increment?: boolean
}

export const commonKeyValueDBFullSupport: CommonKeyValueDBSupport = {
  count: true,
  increment: true,
}
