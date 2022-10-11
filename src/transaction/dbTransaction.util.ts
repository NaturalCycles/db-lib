import type { CommonDB } from '../common.db'
import { CommonDBSaveOptions, DBOperation } from '../db.model'
import { DBTransaction } from './dbTransaction'

/**
 * Optimizes the Transaction (list of DBOperations) to do less operations.
 * E.g if you save id1 first and then delete it - this function will turn it into a no-op (self-eliminate).
 * UPD: actually, it will only keep delete, but remove previous ops.
 *
 * Currently only takes into account SaveBatch and DeleteByIds ops.
 * Output ops are maximum 1 per entity - save or delete.
 */
export function mergeDBOperations(ops: DBOperation[]): DBOperation[] {
  return ops // currently "does nothing"
}

// Commented out as "overly complicated"
/*
export function mergeDBOperations(ops: DBOperation[]): DBOperation[] {
  if (ops.length <= 1) return ops // nothing to optimize there

  // This map will be saved in the end. Null would mean "delete"
  // saveMap[table][id] => row
  const data: StringMap<StringMap<ObjectWithId | null>> = {}

  // Merge ops using `saveMap`
  ops.forEach(op => {
    data[op.table] ||= {}

    if (op.type === 'saveBatch') {
      op.rows.forEach(r => (data[op.table]![r.id] = r))
    } else if (op.type === 'deleteByIds') {
      op.ids.forEach(id => (data[op.table]![id] = null))
    } else {
      throw new Error(`DBOperation not supported: ${(op as any).type}`)
    }
  })

  const resultOps: DBOperation[] = []

  _stringMapEntries(data).forEach(([table, map]) => {
    const saveOp: DBSaveBatchOperation = {
      type: 'saveBatch',
      table,
      rows: _stringMapValues(map).filter(_isTruthy),
    }

    if (saveOp.rows.length) {
      resultOps.push(saveOp)
    }

    const deleteOp: DBDeleteByIdsOperation = {
      type: 'deleteByIds',
      table,
      ids: _stringMapEntries(map).filter(([id, row]) => row === null).map(([id]) => id),
    }

    if (deleteOp.ids.length) {
      resultOps.push(deleteOp)
    }
  })

  return resultOps
}
 */

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
  // const ops = mergeDBOperations(tx.ops)

  for await (const op of tx.ops) {
    if (op.type === 'saveBatch') {
      await db.saveBatch(op.table, op.rows, { ...op.opt, ...opt })
    } else if (op.type === 'deleteByIds') {
      await db.deleteByIds(op.table, op.ids, { ...op.opt, ...opt })
    } else {
      throw new Error(`DBOperation not supported: ${(op as any).type}`)
    }
  }
}
