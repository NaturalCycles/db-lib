import { CommonDB } from '../common.db'
import { CommonDBOptions, CommonDBSaveOptions, ObjectWithId } from '../db.model'

/**
 * DB Transaction has 2 concerns:
 *
 * 1. Group/buffer multiple operations to be executed "at once" (batch execution).
 * 2. All-or-nothing execution. If at least 1 operation fails - whole "transaction" is rolled back as if it never happened.
 */
export class DBTransaction {
  constructor(public db: CommonDB) {}

  _ops: DBOperation[] = []

  saveBatch<ROW extends ObjectWithId>(table: string, rows: ROW[], opt?: CommonDBSaveOptions): this {
    this._ops.push({
      type: 'saveBatch',
      table,
      rows,
      opt,
    })

    return this
  }

  deleteByIds(table: string, ids: string[], opt?: CommonDBOptions): this {
    this._ops.push({
      type: 'deleteByIds',
      table,
      ids,
      opt,
    })

    return this
  }

  /**
   * Default implementation will simply replay all operations in the right order
   * with concurrency of 1 (serially).
   *
   * Override this method in your CommonDB implementation to support "native" DB transactions.
   */
  async commit(): Promise<void> {
    for await (const op of this._ops) {
      if (op.type === 'saveBatch') {
        await this.db.saveBatch(op.table, op.rows, op.opt)
      } else if (op.type === 'deleteByIds') {
        await this.db.deleteByIds(op.table, op.ids, op.opt)
      } else {
        throw new Error(`DBOperation not supported: ${op!.type}`)
      }
    }
  }
}

export type DBOperation = DBSaveBatchOperation | DBDeleteByIdsOperation

export interface DBSaveBatchOperation<ROW extends ObjectWithId = any> {
  type: 'saveBatch'
  table: string
  rows: ROW[]
  opt?: CommonDBSaveOptions
}

export interface DBDeleteByIdsOperation {
  type: 'deleteByIds'
  table: string
  ids: string[]
  opt?: CommonDBOptions
}
