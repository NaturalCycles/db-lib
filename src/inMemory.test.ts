import { TEST_TABLE, testDao, testDB, TestItem, testItemSchema } from '@naturalcycles/db-dev-lib'
import { CommonDao } from './common.dao'
import { InMemoryDB } from './inMemory.db'

const db = new InMemoryDB()

const dao = new CommonDao<TestItem>({
  table: TEST_TABLE,
  db,
  dbmSchema: testItemSchema,
  bmSchema: testItemSchema,
})

test('testDB', async () => {
  await testDB(db)
})

test('testDao', async () => {
  await testDao(dao as any)
})
