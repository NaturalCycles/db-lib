import { SimpleFileDB } from './simpleFile.db'
import { tmpDir } from './test/paths.cnst'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'

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
