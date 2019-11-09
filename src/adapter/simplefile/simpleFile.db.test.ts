import { loadSecretsFromEnv } from '@naturalcycles/nodejs-lib'
import { getDB } from '../../getDB'
import { tmpDir } from '../../test/paths.cnst'
import { runCommonDaoTest } from '../../testing/daoTest'
import { runCommonDBTest } from '../../testing/dbTest'
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

test('getDB())', async () => {
  process.env.DB1 = `${process.cwd()}/src/adapter/simplefile`
  process.env.SECRET_DB1 = JSON.stringify({
    storageDir: `${tmpDir}/storage`,
  })
  loadSecretsFromEnv()
  const db = getDB()
  expect(db).toBeInstanceOf(SimpleFileDB)
  await db.getTables()
})
