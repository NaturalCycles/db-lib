import { pMap, StringMap, _by, _uniq } from '@naturalcycles/js-lib'
import { ObjectWithId } from '../../db.model'
import { DBSaveBatchOperation, DBTransaction } from '../../transaction/dbTransaction'
import { FileDB } from './file.db'

/**
 * Merges all Save and Delete operations (since they are performed on Full files).
 */
export class FileDBTransaction extends DBTransaction {
  constructor(public db: FileDB) {
    super(db)
  }

  async commit(): Promise<void> {
    const tables = _uniq(this._ops.map(o => o.table))

    // 1. Load all tables data (concurrently)
    const data: StringMap<StringMap<ObjectWithId>> = {}

    await pMap(
      tables,
      async table => {
        const dbms = await this.db.loadFile(table)
        data[table] = _by(dbms, dbm => dbm.id)
      },
      { concurrency: 16 },
    )

    // 2. Apply ops one by one (in order)
    this._ops.forEach(op => {
      if (op.type === 'deleteByIds') {
        op.ids.forEach(id => delete data[op.table]![id])
      } else if (op.type === 'saveBatch') {
        op.dbms.forEach(dbm => (data[op.table]![dbm.id] = dbm))
      } else {
        throw new Error(`DBOperation not supported: ${op!.type}`)
      }
    })

    // 3. Sort, turn it into ops
    const ops: DBSaveBatchOperation[] = Object.keys(data).map(table => {
      return {
        type: 'saveBatch',
        table,
        dbms: this.db.sortDBMs(Object.values(data[table]!)),
      }
    })

    // 4. Save all files
    await this.db.saveFiles(ops)
  }
}
