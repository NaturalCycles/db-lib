import { StringMap, _by } from '@naturalcycles/js-lib'
import { ObjectWithId } from '../../db.model'
import { DBSaveBatchOperation } from '../../transaction/dbTransaction'
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

  async loadFile<DBM extends ObjectWithId>(table: string): Promise<DBM[]> {
    return Object.values(this.data[table] || ({} as any))
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {
    ops.forEach(op => {
      this.data[op.table] = _by(op.dbms, dbm => dbm.id)
    })
  }
}
