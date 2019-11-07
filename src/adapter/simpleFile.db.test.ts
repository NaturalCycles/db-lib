import { tmpDir } from '../test/paths.cnst'
import { runCommonDaoTest } from '../testing/daoTest'
import { runCommonDBTest } from '../testing/dbTest'
import { SimpleFileDB } from './simpleFile.db'

describe('json', () => {
  const db = new SimpleFileDB({
    storageDir: `${tmpDir}/storage`,
  })

  describe('runCommonDBTest', () => runCommonDBTest(db))

  describe('runCommonDaoTest', () => runCommonDaoTest(db))
})

describe('ndjson', () => {
  const db = new SimpleFileDB({
    storageDir: `${tmpDir}/storage`,
    ndjson: true,
  })

  describe('runCommonDBTest', () => runCommonDBTest(db))

  describe('runCommonDaoTest', () => runCommonDaoTest(db))
})
