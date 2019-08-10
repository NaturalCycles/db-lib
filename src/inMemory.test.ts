import { TEST_TABLE, testDao, testDB, TestItem, testItemSchema } from '@naturalcycles/db-dev-lib'
import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { DBQuery } from './dbQuery'
import { InMemoryDB } from './inMemory.db'

const db = new InMemoryDB()

const dao = new CommonDao<TestItem>({
  table: TEST_TABLE,
  db,
  dbmSchema: testItemSchema,
  bmSchema: testItemSchema,
  logStarted: true,
  logLevel: CommonDaoLogLevel.DATA_FULL,
})

test('testDB', async () => {
  await testDB(db, DBQuery)
})

test('testDao', async () => {
  await testDao(dao as any, DBQuery)
})
