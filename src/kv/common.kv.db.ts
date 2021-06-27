import { StringMap } from '@naturalcycles/js-lib'
import { CommonDBCreateOptions } from '../db.model'

/**
 * Common interface for Key-Value database implementations.
 *
 * @experimental
 */
export interface CommonKVDB {
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
   * Returns an array of values found. Not found values are not returned (no error is thrown).
   */
  getByIds(table: string, ids: string[]): Promise<Buffer[]>

  deleteByIds(table: string, ids: string[]): Promise<void>

  saveBatch(table: string, batch: StringMap<Buffer>): Promise<void>
}

// consider streamIds(table: string, ids: string[])
