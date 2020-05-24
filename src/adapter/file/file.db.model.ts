import type { SavedDBEntity } from '../../db.model'
import type { DBQueryOrder } from '../../dbQuery'
import type { DBTransaction } from '../../dbTransaction'
import type { FileDB } from './file.db'

export interface FileDBPersistencePlugin {
  ping(): Promise<void>
  getTables(): Promise<string[]>
  loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]>
  saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void>
  transaction(db: FileDB): DBTransaction
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

  /**
   * @default false
   */
  logStarted?: boolean

  /**
   * @default true
   */
  logFinished?: boolean
}
