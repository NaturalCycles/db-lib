import { runCommonDaoTest } from '../../testing/daoTest'
import { runCommonDBTest } from '../../testing/dbTest'
import { TEST_TABLE } from '../../testing/test.model'
import { InMemoryDB } from '../inmemory/inMemory.db'
import { CacheDB } from './cache.db'

const downstreamDB = new InMemoryDB()
const cacheDB = new InMemoryDB()
const db = new CacheDB({
  name: 'cache-db',
  cacheDB,
  downstreamDB,
  logCached: true,
  logDownstream: true,
})

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))

test('simple', async () => {
  const _r = await db.getByIds(TEST_TABLE, ['id1'])
  // console.log(r)
})
