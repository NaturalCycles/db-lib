import { runCommonDaoTest, runCommonDBTest } from '../../testing'
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

describe('runCommonDBTest', () =>
  runCommonDBTest(db, {
    transactions: false,
  }))

describe('runCommonDaoTest', () =>
  runCommonDaoTest(db, {
    transactions: false,
  }))
