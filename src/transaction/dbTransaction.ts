import type { CommonDB } from '../common.db'
import type { CommonDBSaveOptions, DBOperation, ObjectWithId } from '../db.model'

/**
 * Convenience class that stores the list of DBOperations and provides a fluent API to add them.
 */
export class DBTransaction {
  public ops: DBOperation[] = []

  saveBatch<ROW extends ObjectWithId = any>(table: string, rows: ROW[]): void {
    this.ops.push({
      type: 'saveBatch',
      table,
      rows,
    })
  }

  deleteByIds(table: string, ids: string[]): void {
    this.ops.push({
      type: 'deleteByIds',
      table,
      ids,
    })
  }
}

/**
 * Extends DBTransaction by providing a convenient `commit` method that delegates
 * to CommonDB.commitTransaction().
 */
export class RunnableDBTransaction extends DBTransaction {
  constructor(public db: CommonDB) {
    super()
  }

  async commit(opt?: CommonDBSaveOptions): Promise<void> {
    await this.db.commitTransaction(this, opt)
  }
}
