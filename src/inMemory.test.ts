import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { InMemoryDB } from './inMemory.db'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'
import { TEST_TABLE, TestItem, testItemSchema } from './testing/test.model'

const db = new InMemoryDB()

const dao = new CommonDao<TestItem>({
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
