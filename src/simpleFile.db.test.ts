import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { SimpleFileDB } from './simpleFile.db'
import { tmpDir } from './test/paths.cnst'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'
import { TEST_TABLE, testItemSchema } from './testing/test.model'

const db = new SimpleFileDB({
  storageDir: `${tmpDir}/storage`,
})

const dao = new CommonDao({
  table: TEST_TABLE,
  db,
  dbmSchema: testItemSchema,
  bmSchema: testItemSchema,
  logStarted: true,
  logLevel: CommonDaoLogLevel.DATA_FULL,
})

test('runCommonDBTest', async () => {
  await runCommonDBTest(db)
})

test('runCommonDaoTest', async () => {
  await runCommonDaoTest(dao)
})
