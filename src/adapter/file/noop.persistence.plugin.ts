import { SavedDBEntity } from '../../db.model'
import { DBTransaction } from '../../dbTransaction'
import { FileDB } from './file.db'
import { FileDBPersistencePlugin } from './file.db.model'

export class NoopPersistencePlugin implements FileDBPersistencePlugin {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    return []
  }

  async saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void> {}

  transaction(db: FileDB): DBTransaction {
    return new DBTransaction(db)
  }
}
