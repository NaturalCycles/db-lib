import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDBCreateOptions } from '../db.model'

export type KeyValueDBTuple = [key: string, value: Buffer]

/**
 * Common interface for Key-Value database implementations.
 *
 * @experimental
 */
export interface CommonKeyValueDB {
  /**
   * Check that DB connection is working properly.
   */
  ping(): Promise<void>

  /**
   * Will do like `create table ...` for mysql.
   * Caution! dropIfExists defaults to false. If set to true - will actually DROP the table!
   */
  createTable(table: string, opt?: CommonDBCreateOptions): Promise<void>

  /**
   * Returns an array of tuples [key, value]. Not found values are not returned (no error is thrown).
   *
   * Currently it is NOT required to maintain the same order as input `ids`.
   */
  getByIds(table: string, ids: string[]): Promise<KeyValueDBTuple[]>

  deleteByIds(table: string, ids: string[]): Promise<void>

  saveBatch(table: string, entries: KeyValueDBTuple[]): Promise<void>

  streamIds(table: string, limit?: number): ReadableTyped<string>
  streamValues(table: string, limit?: number): ReadableTyped<Buffer>
  streamEntries(table: string, limit?: number): ReadableTyped<KeyValueDBTuple>

  count(table: string): Promise<number>
}
