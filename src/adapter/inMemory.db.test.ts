import { runCommonDaoTest } from '../testing/daoTest'
import { runCommonDBTest } from '../testing/dbTest'
import { InMemoryDB } from './inMemory.db'

const db = new InMemoryDB()

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))
