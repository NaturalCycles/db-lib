import { runCommonKeyValueDaoTest, runCommonKeyValueDBTest, TestItemBM } from '../../testing'
import { InMemoryKeyValueDB } from './inMemoryKeyValueDB'

const dbDb = new InMemoryKeyValueDB<Buffer>()
const dbDao = new InMemoryKeyValueDB<TestItemBM>()

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(dbDb))
describe('runCommonKeyValueDaoTest', () => runCommonKeyValueDaoTest(dbDao))
