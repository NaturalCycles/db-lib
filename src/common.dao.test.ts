import { mockTime } from '@naturalcycles/test-lib'
import { CommonDao } from './common.dao'
import { InMemoryDB } from './inMemory.db'
import { mockTestData, TEST_KIND, TestKindBM, testKindUnsavedBMSchema } from './test/testData.mock'

const ID = 'randomDatastoreService1'

const db = new InMemoryDB<TestKindBM>()

const createDao = () => {
  const dao = new CommonDao<TestKindBM>({
    table: TEST_KIND,
    db,
    dbmUnsavedSchema: testKindUnsavedBMSchema,
    bmUnsavedSchema: testKindUnsavedBMSchema,
  })
  dao.createId = () => ID
  return dao
}

beforeEach(async () => {
  jest.restoreAllMocks()
  mockTime()
  await db.resetCache()
})

test('full test', async () => {
  const dao = createDao()

  expect(await dao.getById()).toBeUndefined()
  expect(await dao.getById('id1')).toBeUndefined()
  expect(await dao.getByIds(['id1', 'id2', 'idX'])).toEqual([])
  expect(await dao.getBy('c', true)).toEqual([])

  await dao.saveBatch(mockTestData())

  expect(await dao.getById()).toBeUndefined()
  expect(await dao.getById('id1')).toMatchSnapshot()
  expect(await dao.getByIds(['id1', 'id2', 'idX'])).toMatchSnapshot()
  expect(await dao.getBy('c', true)).toMatchSnapshot()
})
