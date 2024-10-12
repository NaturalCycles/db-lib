import {
  JsonSchemaObject,
  JsonSchemaRootObject,
  ObjectWithId,
  StringMap,
} from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDB, CommonDBSupport, CommonDBType } from './common.db'
import {
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBTransactionOptions,
  DBTransactionFn,
  RunQueryResult,
} from './db.model'
import { DBQuery } from './query/dbQuery'
import { FakeDBTransaction } from './transaction/dbTransaction.util'

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
    _table: string,
  ): Promise<JsonSchemaRootObject<ROW>> {
    throw new Error('getTableSchema is not implemented')
  }

  async createTable<ROW extends ObjectWithId>(
    _table: string,
    _schema: JsonSchemaObject<ROW>,
  ): Promise<void> {
    // no-op
  }

  async getByIds<ROW extends ObjectWithId>(_table: string, _ids: string[]): Promise<ROW[]> {
    throw new Error('getByIds is not implemented')
  }

  async deleteByQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<number> {
    throw new Error('deleteByQuery is not implemented')
  }

  async patchByQuery<ROW extends ObjectWithId>(
    _q: DBQuery<ROW>,
    _patch: Partial<ROW>,
    _opt?: CommonDBOptions,
  ): Promise<number> {
    throw new Error('patchByQuery is not implemented')
  }

  async runQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<RunQueryResult<ROW>> {
    throw new Error('runQuery is not implemented')
  }

  async runQueryCount<ROW extends ObjectWithId>(_q: DBQuery<ROW>): Promise<number> {
    throw new Error('runQueryCount is not implemented')
  }

  async saveBatch<ROW extends ObjectWithId>(
    _table: string,
    _rows: ROW[],
    _opt?: CommonDBSaveOptions<ROW>,
  ): Promise<void> {
    throw new Error('saveBatch is not implemented')
  }

  streamQuery<ROW extends ObjectWithId>(_q: DBQuery<ROW>): ReadableTyped<ROW> {
    throw new Error('streamQuery is not implemented')
  }

  async deleteByIds(_table: string, _ids: string[], _opt?: CommonDBOptions): Promise<number> {
    throw new Error('deleteByIds is not implemented')
  }

  async runInTransaction(fn: DBTransactionFn, _opt?: CommonDBTransactionOptions): Promise<void> {
    const tx = new FakeDBTransaction(this)
    await fn(tx)
    // there's no try/catch and rollback, as there's nothing to rollback
  }

  async incrementBatch(
    _table: string,
    _prop: string,
    _incrementMap: StringMap<number>,
    _opt?: CommonDBOptions,
  ): Promise<StringMap<number>> {
    throw new Error('incrementBatch is not implemented')
  }
}
