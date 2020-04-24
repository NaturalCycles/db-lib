import { mockTime } from '@naturalcycles/dev-lib/dist/testing'
import { ErrorMode } from '@naturalcycles/js-lib'
import { writableForEach, _pipeline } from '@naturalcycles/nodejs-lib'
import { InMemoryDB } from './adapter/inmemory/inMemory.db'
import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { CommonDaoStreamOptions } from './db.model'
import {
  createTestItemsBM,
  TestItemBM,
  testItemBMSchema,
  TestItemDBM,
  testItemDBMSchema,
  TestItemTM,
  testItemTMSchema,
  TEST_TABLE,
} from './testing/test.model'

let throwError = false

class TestItemDao extends CommonDao<TestItemBM, TestItemDBM, TestItemTM> {
  parseNaturalId(id: string): Partial<TestItemDBM> {
    if (throwError && id === 'id3') throw new Error('error_from_parseNaturalId')

    return {}
  }

  async beforeDBMToBM(dbm: TestItemDBM): Promise<TestItemBM> {
    // if(throwError && dbm.id === 'id4') throw new Error('error_from_beforeDBMToBM')

    return {
      ...dbm,
    }
  }
}

const db = new InMemoryDB()

const dao = new TestItemDao({
  table: TEST_TABLE,
  db,
  dbmSchema: testItemDBMSchema,
  bmSchema: testItemBMSchema,
  tmSchema: testItemTMSchema,
  // logStarted: true,
  logLevel: CommonDaoLogLevel.OPERATIONS,
})

beforeEach(async () => {
  await db.resetCache()
  mockTime()
})

test('should propagate pipe errors', async () => {
  const items = createTestItemsBM(20)

  await dao.saveBatch(items, {
    preserveUpdatedCreated: true,
  })

  throwError = true

  const opt: CommonDaoStreamOptions = {
    // logEvery: 1,
  }

  // default: Suppress errors
  let results: any[] = []
  await dao.query().streamQueryForEach(r => void results.push(r), opt)
  // console.log(results)
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))

  // Suppress errors
  results = []
  await dao
    .query()
    .streamQueryForEach(r => void results.push(r), { ...opt, errorMode: ErrorMode.SUPPRESS })
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))

  // THROW_IMMEDIATELY
  results = []
  await expect(
    dao.query().streamQueryForEach(r => void results.push(r), {
      ...opt,
      errorMode: ErrorMode.THROW_IMMEDIATELY,
    }),
  ).rejects.toThrow('error_from_parseNaturalId')
  expect(results).toEqual([])

  // THROW_AGGREGATED
  results = []
  await expect(
    dao.query().streamQueryForEach(r => void results.push(r), {
      ...opt,
      errorMode: ErrorMode.THROW_AGGREGATED,
    }),
  ).rejects.toThrow('error_from_parseNaturalId')
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))

  // .stream should suppress by default
  results = []
  await _pipeline([dao.query().streamQuery(opt), writableForEach(r => void results.push(r))])
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))
})

test('patch', async () => {
  const id = '123456'
  const r = await dao.patch(id, {
    k1: 'k111',
  })

  const r2 = await dao.getById(id)

  expect(r.id).toBe(id)
  expect(r2).toEqual(r)
  expect(r).toMatchSnapshot()
})
