import { CommonLogger, ObjectWithId } from '@naturalcycles/js-lib'
import { DBSaveBatchOperation } from '../../db.model'
import type { DBQueryOrder } from '../../query/dbQuery'

export interface FileDBPersistencePlugin {
  ping: () => Promise<void>
  getTables: () => Promise<string[]>
  loadFile: <ROW extends ObjectWithId>(table: string) => Promise<ROW[]>
  saveFiles: (ops: DBSaveBatchOperation<any>[]) => Promise<void>
}

export interface FileDBCfg {
  plugin: FileDBPersistencePlugin

  /**
   * @default undefined, which means "insertion order"
   */
  sortOnSave?: DBQueryOrder<any>

  /**
   * @default true
   * If true - will run `sortObjectDeep()` on each object to achieve deterministic sort
   */
  sortObjects?: boolean

  /**
   * Defaults to `console`.
   */
  logger?: CommonLogger

  /**
   * @default false
   */
  logStarted?: boolean

  /**
   * @default true
   */
  logFinished?: boolean
}
