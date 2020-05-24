import { StringMap, _by } from '@naturalcycles/js-lib'
import { SavedDBEntity } from '../../db.model'
import { DBSaveBatchOperation } from '../../dbTransaction'
import { FileDBPersistencePlugin } from './file.db.model'

/**
 * Mostly useful for testing.
 */
export class InMemoryPersistencePlugin implements FileDBPersistencePlugin {
  data: StringMap<StringMap<SavedDBEntity>> = {}

  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return Object.keys(this.data)
  }

  async loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    return Object.values(this.data[table] || ({} as any))
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {
    ops.forEach(op => {
      this.data[op.table] = _by(op.dbms, dbm => dbm.id)
    })
  }
}
