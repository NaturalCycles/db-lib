import { StringMap, _by } from '@naturalcycles/js-lib'
import { SavedDBEntity } from '../../db.model'
import { DBTransaction } from '../../dbTransaction'
import { FileDB } from './file.db'
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

  async saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void> {
    this.data[table] = _by(dbms, dbm => dbm.id)
  }

  transaction(db: FileDB): DBTransaction {
    return new DBTransaction(db)
  }
}
