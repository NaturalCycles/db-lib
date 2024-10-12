import { CommonKeyValueDao } from '../../kv/commonKeyValueDao'
import { runCommonKeyValueDaoTest, runCommonKeyValueDBTest, TEST_TABLE } from '../../testing'
import { InMemoryKeyValueDB } from './inMemoryKeyValueDB'

const db = new InMemoryKeyValueDB()
const dao = new CommonKeyValueDao<Buffer>({
  db,
  table: TEST_TABLE,
})

describe('runCommonKeyValueDBTest', () => runCommonKeyValueDBTest(db))
describe('runCommonKeyValueDaoTest', () => runCommonKeyValueDaoTest(dao))
