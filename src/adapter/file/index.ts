import { FileDB } from './file.db'
import type { FileDBCfg, FileDBPersistencePlugin } from './file.db.model'
import { LocalFilePersistencePlugin } from './localFile.persistence.plugin'

export type { FileDBCfg, FileDBPersistencePlugin }
export { FileDB, LocalFilePersistencePlugin }
