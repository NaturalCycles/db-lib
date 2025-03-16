import { describe, expect, test } from 'vitest'
import { createTestItemsDBM, runCommonDaoTest, runCommonDBTest, TEST_TABLE } from '../../testing'
import { InMemoryDB } from './inMemory.db'

const db = new InMemoryDB()

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))

test('persistence', async () => {
  const testItems = createTestItemsDBM(50)

  db.cfg.persistenceEnabled = true
  // db.cfg.persistZip = false

  await db.resetCache()
  await db.saveBatch(TEST_TABLE, testItems)
  const data1 = db.getDataSnapshot()

  await db.flushToDisk()

  await db.restoreFromDisk()
  const data2 = db.getDataSnapshot()

  expect(data2).toEqual(data1) // same data restored

  // cleanup
  await db.resetCache()
  await db.flushToDisk()
})
