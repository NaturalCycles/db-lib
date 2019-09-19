import { CacheDB } from './cache.db'
import { InMemoryDB } from './inMemory.db'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'
import { TEST_TABLE } from './testing/test.model'

const downstreamDB = new InMemoryDB()
const cacheDB = new InMemoryDB()
const db = new CacheDB({
  name: 'cache-db',
  cacheDB,
  downstreamDB,
  logCached: true,
  logDownstream: true,
})

test('runCommonDBTest', async () => {
  await runCommonDBTest(db)
})

test('runCommonDaoTest', async () => {
  await runCommonDaoTest(db)
})

test('simple', async () => {
  const _r = await db.getByIds(TEST_TABLE, ['id1'])
  // console.log(r)
})
