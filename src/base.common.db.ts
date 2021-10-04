import { Readable } from 'stream'
import { JsonSchemaObject } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDB } from './common.db'
import { CommonDBSaveOptions, ObjectWithId, RunQueryResult } from './db.model'
import { DBQuery } from './query/dbQuery'
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

  async getTableSchema<ROW extends ObjectWithId>(table: string): Promise<JsonSchemaObject<ROW>> {
    return {
      $id: `${table}.schema.json`,
      type: 'object',
      additionalProperties: true,
      properties: {} as any,
      required: [],
    }
  }

  async createTable(_table: string, _schema: JsonSchemaObject): Promise<void> {}

  async deleteByIds(_table: string, _ids: string[]): Promise<number> {
    return 0
  }

  async deleteByQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<number> {
    return 0
  }

  async getByIds<ROW extends ObjectWithId>(_table: string, _ids: string[]): Promise<ROW[]> {
    return []
  }

  async runQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<RunQueryResult<ROW>> {
    return { rows: [] }
  }

  async runQueryCount<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<number> {
    return 0
  }

  async saveBatch<ROW extends ObjectWithId>(_table: string, _rows: ROW[]): Promise<void> {}

  streamQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): ReadableTyped<ROW> {
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
