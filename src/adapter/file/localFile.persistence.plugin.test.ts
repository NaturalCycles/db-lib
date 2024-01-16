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
    insert: false,
    update: false,
    updateByQuery: false,
    createTable: false,
    transactions: false,
  }))

describe('runCommonDaoTest', () =>
  runCommonDaoTest(db, {
    createTable: false,
    transactions: false,
  }))
