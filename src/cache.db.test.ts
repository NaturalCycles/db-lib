import { CacheDB } from './cache.db'
import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { InMemoryDB } from './inMemory.db'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'
import { TEST_TABLE, TestItem, testItemSchema } from './testing/test.model'

const downstreamDB = new InMemoryDB()
const cacheDB = new InMemoryDB()
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

test('simple', async () => {
  const _r = await db.getByIds(TEST_TABLE, ['id1'])
  // console.log(r)
})
