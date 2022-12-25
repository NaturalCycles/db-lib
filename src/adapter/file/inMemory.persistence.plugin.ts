import { StringMap, _by, ObjectWithId } from '@naturalcycles/js-lib'
import { DBSaveBatchOperation } from '../../db.model'
import { FileDBPersistencePlugin } from './file.db.model'

/**
 * Mostly useful for testing.
 */
export class InMemoryPersistencePlugin implements FileDBPersistencePlugin {
  data: StringMap<StringMap<ObjectWithId>> = {}

  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return Object.keys(this.data)
  }

  async loadFile<ROW extends ObjectWithId>(table: string): Promise<ROW[]> {
    return Object.values(this.data[table] || ({} as any))
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {
    ops.forEach(op => {
      this.data[op.table] = _by(op.rows, r => r.id)
    })
  }
}
