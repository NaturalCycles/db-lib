import { describe } from 'vitest'
import { runCommonDaoTest, runCommonDBTest } from '../../testing'
import { FileDB } from './file.db'
import { InMemoryPersistencePlugin } from './inMemory.persistence.plugin'

const db = new FileDB({
  plugin: new InMemoryPersistencePlugin(),
})

describe('runCommonDBTest', () => runCommonDBTest(db))

describe('runCommonDaoTest', () => runCommonDaoTest(db))
