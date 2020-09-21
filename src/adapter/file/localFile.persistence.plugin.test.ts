import { runCommonDaoTest, runCommonDBTest } from '../../testing'
import { FileDB } from './file.db'
import { LocalFilePersistencePlugin } from './localFile.persistence.plugin'

const db = new FileDB({
  plugin: new LocalFilePersistencePlugin({
    gzip: false,
  }),
})

describe('runCommonDBTest', () =>
  runCommonDBTest(db, {
    bufferSupport: false, // todo: use bufferReviver
  }))

describe('runCommonDaoTest', () => runCommonDaoTest(db))
