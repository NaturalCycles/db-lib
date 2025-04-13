import { describe } from 'vitest'
import { runCommonDaoTest, runCommonDBTest } from '../../testing/index.js'
import { FileDB } from './file.db.js'
import { LocalFilePersistencePlugin } from './localFile.persistence.plugin.js'

const db = new FileDB({
  plugin: new LocalFilePersistencePlugin({
    gzip: false,
  }),
})

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))
