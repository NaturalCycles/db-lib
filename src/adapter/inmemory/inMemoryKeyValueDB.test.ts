import { describe } from 'vitest'
import { runCommonKeyValueDaoTest, runCommonKeyValueDBTest } from '../../testing/index.js'
import { InMemoryKeyValueDB } from './inMemoryKeyValueDB.js'

const db = new InMemoryKeyValueDB()

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))
describe('runCommonKeyValueDaoTest', () => runCommonKeyValueDaoTest(db))
