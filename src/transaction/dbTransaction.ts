import { AnyObjectWithId, ObjectWithId } from '@naturalcycles/js-lib'
import type { CommonDB } from '../common.db'
import type { CommonDBSaveOptions, DBOperation } from '../db.model'

/**
 * Convenience class that stores the list of DBOperations and provides a fluent API to add them.
 */
export class DBTransaction {
  public ops: DBOperation[] = []

  saveBatch<ROW extends ObjectWithId = AnyObjectWithId>(table: string, rows: ROW[]): this {
    this.ops.push({
      type: 'saveBatch',
      table,
      rows,
    })
    return this
  }

  deleteByIds(table: string, ids: string[]): this {
    this.ops.push({
      type: 'deleteByIds',
      table,
      ids,
    })
    return this
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

  async commit<ROW extends ObjectWithId>(opt?: CommonDBSaveOptions<ROW>): Promise<void> {
    await this.db.commitTransaction(this, opt)
  }
}
