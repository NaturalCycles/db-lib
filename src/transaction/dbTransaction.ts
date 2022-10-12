import { ObjectWithId } from '@naturalcycles/js-lib'
import type { CommonDB } from '../common.db'
import type { CommonDBSaveOptions, DBOperation } from '../db.model'

/**
 * Convenience class that stores the list of DBOperations and provides a fluent API to add them.
 */
export class DBTransaction {
  protected constructor(public ops: DBOperation[] = []) {}

  /**
   * Convenience method.
   */
  static create(ops: DBOperation[] = []): DBTransaction {
    return new DBTransaction(ops)
  }

  save<ROW extends Partial<ObjectWithId>>(table: string, row: ROW): this {
    this.ops.push({
      type: 'saveBatch',
      table,
      rows: [row],
    })
    return this
  }

  saveBatch<ROW extends Partial<ObjectWithId>>(table: string, rows: ROW[]): this {
    this.ops.push({
      type: 'saveBatch',
      table,
      rows,
    })
    return this
  }

  deleteById(table: string, id: string): this {
    this.ops.push({
      type: 'deleteByIds',
      table,
      ids: [id],
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

  async commit<ROW extends Partial<ObjectWithId>>(opt?: CommonDBSaveOptions<ROW>): Promise<void> {
    await this.db.commitTransaction(this, opt)
  }
}
