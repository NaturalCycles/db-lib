import { SavedDBEntity } from '../../db.model'
import { DBSaveBatchOperation } from '../../dbTransaction'
import { FileDBPersistencePlugin } from './file.db.model'

export class NoopPersistencePlugin implements FileDBPersistencePlugin {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    return []
  }

  async saveFiles(ops: DBSaveBatchOperation[]): Promise<void> {}
}