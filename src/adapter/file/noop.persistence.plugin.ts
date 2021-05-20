import { DBSaveBatchOperation, ObjectWithId } from '../../db.model'
import { FileDBPersistencePlugin } from './file.db.model'

export class NoopPersistencePlugin implements FileDBPersistencePlugin {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async loadFile<ROW extends ObjectWithId>(_table: string): Promise<ROW[]> {
    return []
  }

  async saveFiles(_ops: DBSaveBatchOperation[]): Promise<void> {}
}
