import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { Readable } from 'stream'
import { CommonDB } from './common.db'
import {
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  ObjectWithId,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'
import { CommonSchema } from './schema/common.schema'
import { DBTransaction } from './transaction/dbTransaction'
import { commitDBTransactionSimple } from './transaction/dbTransaction.util'

/**
 * No-op implementation of CommonDB interface.
 * To be extended by actual implementations.
 */
export class BaseCommonDB implements CommonDB {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async getTableSchema<ROW>(table: string): Promise<CommonSchema<ROW>> {
    return { table, fields: [] }
  }

  async createTable(schema: CommonSchema, opt?: CommonDBCreateOptions): Promise<void> {}

  async deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    return 0
  }

  async deleteByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    return 0
  }

  async getByIds<ROW extends ObjectWithId>(
    table: string,
    ids: string[],
    opt?: CommonDBOptions,
  ): Promise<ROW[]> {
    return []
  }

  async runQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<RunQueryResult<ROW>> {
    return { rows: [] }
  }

  async runQueryCount<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    return 0
  }

  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions,
  ): Promise<void> {}

  streamQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    opt?: CommonDBOptions,
  ): ReadableTyped<ROW> {
    return Readable.from([])
  }

  /**
   * Naive implementation.
   * To be extended.
   */
  async commitTransaction(tx: DBTransaction, opt?: CommonDBSaveOptions): Promise<void> {
    await commitDBTransactionSimple(this, tx, opt)
  }
}
