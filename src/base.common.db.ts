import { JsonSchemaObject, JsonSchemaRootObject, ObjectWithId } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDB, CommonDBSupport, CommonDBType } from './common.db'
import {
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBTransactionOptions,
  DBPatch,
  DBTransactionFn,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'
import { FakeDBTransaction } from './transaction/dbTransaction.util'

/* eslint-disable unused-imports/no-unused-vars */

/**
 * No-op implementation of CommonDB interface.
 * To be extended by actual implementations.
 */
export class BaseCommonDB implements CommonDB {
  dbType = CommonDBType.document

  support: CommonDBSupport = {}

  async ping(): Promise<void> {
    throw new Error('ping is not implemented')
  }

  async getTables(): Promise<string[]> {
    throw new Error('getTables is not implemented')
  }

  async getTableSchema<ROW extends ObjectWithId>(
    table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    throw new Error('getTableSchema is not implemented')
  }

  async createTable<ROW extends ObjectWithId>(
    table: string,
    schema: JsonSchemaObject<ROW>,
  ): Promise<void> {
    // no-op
  }

  async getByIds<ROW extends ObjectWithId>(table: string, ids: string[]): Promise<ROW[]> {
    throw new Error('getByIds is not implemented')
  }

  async deleteByQuery<ROW extends ObjectWithId>(q: DBQuery<ROW>): Promise<number> {
    throw new Error('deleteByQuery is not implemented')
  }

  async updateByQuery<ROW extends ObjectWithId>(
    q: DBQuery<ROW>,
    patch: DBPatch<ROW>,
    opt?: CommonDBOptions,
  ): Promise<number> {
    throw new Error('updateByQuery is not implemented')
  }

  async runQuery<ROW extends ObjectWithId>(q: DBQuery<ROW>): Promise<RunQueryResult<ROW>> {
    throw new Error('runQuery is not implemented')
  }

  async runQueryCount<ROW extends ObjectWithId>(q: DBQuery<ROW>): Promise<number> {
    throw new Error('runQueryCount is not implemented')
  }

  async saveBatch<ROW extends ObjectWithId>(
    table: string,
    rows: ROW[],
    opt?: CommonDBSaveOptions<ROW>,
  ): Promise<void> {
    throw new Error('saveBatch is not implemented')
  }

  streamQuery<ROW extends ObjectWithId>(q: DBQuery<ROW>): ReadableTyped<ROW> {
    throw new Error('streamQuery is not implemented')
  }

  async deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): Promise<number> {
    throw new Error('deleteByIds is not implemented')
  }

  async runInTransaction(fn: DBTransactionFn, opt?: CommonDBTransactionOptions): Promise<void> {
    const tx = new FakeDBTransaction(this)
    await fn(tx)
    // there's no try/catch and rollback, as there's nothing to rollback
  }
}
