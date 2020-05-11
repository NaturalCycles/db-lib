import { SavedDBEntity } from '../../db.model'
import { DBQueryOrder } from '../../dbQuery'

export interface FileDBPersistencePlugin {
  ping(): Promise<void>
  getTables(): Promise<string[]>
  loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]>
  saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void>
}

export interface FileDBCfg {
  plugin: FileDBPersistencePlugin

  /**
   * @default undefined, which means "insertion order"
   */
  sortOnSave?: DBQueryOrder

  /**
   * @default true
   * If true - will run `sortObjectDeep()` on each object to achieve deterministic sort
   */
  sortObjects?: boolean
}
