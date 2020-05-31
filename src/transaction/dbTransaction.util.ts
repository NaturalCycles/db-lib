import { StringMap } from '@naturalcycles/js-lib'
import type { CommonDB } from '../common.db'
import { CommonDBSaveOptions, DBOperation, ObjectWithId } from '../db.model'
import { DBTransaction } from './dbTransaction'

/**
 * Optimizes the Transaction (list of DBOperations) to do less operations.
 * E.g if you save id1 first and then delete it - this function will turn it into a no-op (self-eliminate).
 *
 * Currently only takes into account SaveBatch and DeleteByIds ops.
 * Output ops are maximum 2 (per table) - save and delete (where order actually doesn't matter, cause ids there will not overlap).
 */
export function mergeDBOperations(ops: DBOperation[]): DBOperation[] {
  if (ops.length <= 1) return ops // nothing to optimize there

  // This map will be saved in the end. Null would mean "delete"
  // saveMap[table][id] => row
  const saveMapByTable: StringMap<StringMap<ObjectWithId | null>> = {}

  // Merge ops using `saveMap`
  ops.forEach(op => {
    saveMapByTable[op.table] = saveMapByTable[op.table] || {}

    if (op.type === 'saveBatch') {
      op.rows.forEach(r => (saveMapByTable[op.table]![r.id] = r))
    } else if (op.type === 'deleteByIds') {
      op.ids.forEach(id => (saveMapByTable[op.table]![id] = null))
    } else {
      throw new Error(`DBOperation not supported: ${op!.type}`)
    }
  })

  const resultOps: DBOperation[] = []

  Object.entries(saveMapByTable).forEach(([table, saveMap]) => {
    const rowsToSave: ObjectWithId[] = []
    const idsToDelete: string[] = []

    Object.entries(saveMap!).forEach(([id, r]) => {
      if (r === null) {
        idsToDelete.push(id)
      } else {
        rowsToSave.push(r!)
      }
    })

    if (rowsToSave.length) {
      resultOps.push({
        type: 'saveBatch',
        table,
        rows: rowsToSave,
      })
    }

    if (idsToDelete.length) {
      resultOps.push({
        type: 'deleteByIds',
        table,
        ids: idsToDelete,
      })
    }
  })

  return resultOps
}

/**
 * Naive implementation of "Transaction" which just executes all operations one-by-one.
 * Does NOT actually implement a Transaction, cause partial ops application will happen
 * in case of an error in the middle.
 */
export async function commitDBTransactionSimple(
  db: CommonDB,
  tx: DBTransaction,
  opt?: CommonDBSaveOptions,
): Promise<void> {
  const ops = mergeDBOperations(tx.ops)

  for await (const op of ops) {
    if (op.type === 'saveBatch') {
      await db.saveBatch(op.table, op.rows, opt)
    } else if (op.type === 'deleteByIds') {
      await db.deleteByIds(op.table, op.ids, opt)
    } else {
      throw new Error(`DBOperation not supported: ${op!.type}`)
    }
  }
}
