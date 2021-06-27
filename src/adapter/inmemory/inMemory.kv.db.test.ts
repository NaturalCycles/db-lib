import { runCommonKVDBTest } from '../../testing/kvDBTest'
import { InMemoryKVDB } from './inMemory.kv.db'

const db = new InMemoryKVDB()

describe('runCommonKVDBTest', () => runCommonKVDBTest(db))
