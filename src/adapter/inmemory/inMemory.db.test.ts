import { loadSecretsFromEnv } from '@naturalcycles/nodejs-lib'
import { getDB } from '../../getDB'
import { runCommonDaoTest } from '../../testing/daoTest'
import { runCommonDBTest } from '../../testing/dbTest'
import { InMemoryDB } from './inMemory.db'

const db = new InMemoryDB()

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))

test('getDB())', async () => {
  loadSecretsFromEnv()
  process.env.DB1 = `${process.cwd()}/src/adapter/inmemory`
  const db = getDB()
  expect(db).toBeInstanceOf(InMemoryDB)
  expect(await db.getTables()).toEqual([])
})
