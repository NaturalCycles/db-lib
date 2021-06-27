import { CommonKVDao } from '../../kv/common.kv.dao'
import { TEST_TABLE } from '../../testing'
import { runCommonKVDBTest } from '../../testing'
import { runCommonKVDaoTest } from '../../testing/kvDaoTest'
import { InMemoryKVDB } from './inMemory.kv.db'

const db = new InMemoryKVDB()
const dao = new CommonKVDao<Buffer>({
  db,
  table: TEST_TABLE,
})

describe('runCommonKVDBTest', () => runCommonKVDBTest(db))
describe('runCommonKVDaoTest', () => runCommonKVDaoTest(dao))
