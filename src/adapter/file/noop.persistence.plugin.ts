import { PartialObjectWithId, Saved } from '@naturalcycles/js-lib'
import { DBSaveBatchOperation } from '../../db.model'
import { FileDBPersistencePlugin } from './file.db.model'

export class NoopPersistencePlugin implements FileDBPersistencePlugin {
  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return []
  }

  async loadFile<ROW extends PartialObjectWithId>(_table: string): Promise<Saved<ROW>[]> {
    return []
  }

  async saveFiles(_ops: DBSaveBatchOperation[]): Promise<void> {}
}
