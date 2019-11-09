import { ErrorMode } from '@naturalcycles/js-lib'
import { _pipeline, writableForEach } from '@naturalcycles/nodejs-lib'
import { InMemoryDB } from './adapter/inMemory.db'
import { CommonDao, CommonDaoLogLevel } from './common.dao'
import { CommonDaoStreamOptions } from './db.model'
import {
  createTestItemsBM,
  TEST_TABLE,
  TestItemBM,
  testItemBMSchema,
  TestItemDBM,
  testItemDBMSchema,
  TestItemTM,
  testItemTMSchema,
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

test('should propagate pipe errors', async () => {
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
