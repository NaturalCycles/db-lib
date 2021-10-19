import { ObjectWithId } from '@naturalcycles/js-lib'
import { DBSaveBatchOperation } from '../../db.model'
import type { DBQueryOrder } from '../../query/dbQuery'

export interface FileDBPersistencePlugin {
  ping(): Promise<void>
  getTables(): Promise<string[]>
  loadFile<ROW extends ObjectWithId>(table: string): Promise<ROW[]>
  saveFiles(ops: DBSaveBatchOperation[]): Promise<void>
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
