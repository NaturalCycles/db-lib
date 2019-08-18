import {
  TEST_TABLE,
  testDao,
  testDB,
  TestItem,
  testItemUnsavedSchema,
} from '@naturalcycles/db-dev-lib'
import { CacheDB } from './cache.db'
import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { DBQuery } from './dbQuery'
import { InMemoryDB } from './inMemory.db'

const downstreamDB = new InMemoryDB<TestItem>()
const cacheDB = new InMemoryDB<TestItem>()
const db = new CacheDB({
  name: 'cache-db',
  cacheDB,
  downstreamDB,
  logCached: true,
  logDownstream: true,
})

const dao = new CommonDao<TestItem>({
  table: TEST_TABLE,
  db,
  dbmUnsavedSchema: testItemUnsavedSchema,
  bmUnsavedSchema: testItemUnsavedSchema,
  logStarted: true,
  logLevel: CommonDaoLogLevel.DATA_FULL,
})

test('testDB', async () => {
  await testDB(db as any, DBQuery)
})

test('testDao', async () => {
  await testDao(dao as any, DBQuery)
})

test('simple', async () => {
  const _r = await db.getByIds(TEST_TABLE, ['id1'])
  // console.log(r)
})
