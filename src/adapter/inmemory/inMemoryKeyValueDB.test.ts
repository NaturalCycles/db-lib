import { describe } from 'vitest'
import { runCommonKeyValueDaoTest, runCommonKeyValueDBTest } from '../../testing'
import { InMemoryKeyValueDB } from './inMemoryKeyValueDB'

const db = new InMemoryKeyValueDB()

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))
describe('runCommonKeyValueDaoTest', () => runCommonKeyValueDaoTest(db))
