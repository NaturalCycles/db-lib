import { mockTime, MOCK_TS_2018_06_21 } from '@naturalcycles/dev-lib/dist/testing'
import { ErrorMode, _omit } from '@naturalcycles/js-lib'
import { writableForEach, _pipeline } from '@naturalcycles/nodejs-lib'
import { InMemoryDB } from '../adapter/inmemory/inMemory.db'
import { DBLibError } from '../cnst'
import {
  createTestItemsBM,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
  TEST_TABLE,
} from '../testing/test.model'
import { CommonDao } from './common.dao'
import { CommonDaoLogLevel, CommonDaoSaveOptions } from './common.dao.model'

let throwError = false

const db = new InMemoryDB()

const dao = new CommonDao({
  table: TEST_TABLE,
  db,
  dbmSchema: testItemDBMSchema,
  bmSchema: testItemBMSchema,
  tmSchema: testItemTMSchema,
  // logStarted: true,
  logLevel: CommonDaoLogLevel.OPERATIONS,
  hooks: {
    parseNaturalId: id => {
      if (throwError && id === 'id3') throw new Error('error_from_parseNaturalId')

      return {}
    },
    beforeDBMToBM: dbm => {
      // if(throwError && dbm.id === 'id4') throw new Error('error_from_beforeDBMToBM')

      return {
        ...dbm,
      }
    },
  },
})

beforeEach(async () => {
  jest.resetAllMocks()
  await db.resetCache()
  mockTime()
})

test('common', async () => {
  // This also tests type overloads (infers `null` if input is undefined)
  // expect(await dao.getById()).toBeNull() // illegal
  expect(await dao.getById(undefined)).toBeNull()
  expect(await dao.getById('non-existing')).toBeNull()
  expect(await dao.getByIdAsDBM(undefined)).toBeNull()
  expect(await dao.getByIdAsDBM('123')).toBeNull()
  expect(await dao.getByIdAsTM(undefined)).toBeNull()
  expect(await dao.getByIdAsTM('123')).toBeNull()

  expect(await dao.deleteById(undefined)).toBe(0)
  expect(await dao.deleteById('123')).toBe(0)

  expect(dao.anyToDBM(undefined)).toBeUndefined()
  expect(dao.anyToDBM({})).toMatchObject({})
})

test('should propagate pipe errors', async () => {
  const items = createTestItemsBM(20)

  await dao.saveBatch(items, {
    preserveUpdatedCreated: true,
  })

  throwError = true

  const opt = {
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

// todo: fix jest mock
test.skip('ensureUniqueId', async () => {
  const opt: CommonDaoSaveOptions = {
    ensureUniqueId: true,
  }

  // Will be autogenerated (both items)
  const [item1, item2, item3] = createTestItemsBM(3).map(r => _omit(r, ['id']))
  const item1Saved = await dao.save(item1!, opt)
  const { id: id1 } = item1Saved

  const _item2Saved = await dao.save(item2!, opt)
  // const { id: id2 } = item2Saved

  // Saving existing is fine
  await dao.save(item1!, opt)

  // Mock generator to make it generate same id as id1
  jest.spyOn(require('@naturalcycles/nodejs-lib'), 'stringId').mockImplementationOnce(() => {
    return id1
  })

  // verify mocking works
  // expect(stringId()).toBe(id1)

  // Save with same id should throw now!
  await expect(dao.save(item3!, opt)).rejects.toThrow(DBLibError.NON_UNIQUE_ID)

  // Emulate "retry" - should work now, cause mock only runs once
  await dao.save(item3!, opt)
})

test('should strip null on load and save', async () => {
  const r = await dao.save({
    id: '123',
    k1: 'k1',
    k2: null as any,
  })

  // console.log(r)

  // r is mutated with created/updated properties, but null values are intact
  expect(r).toEqual({
    id: '123',
    k1: 'k1',
    k2: null,
    created: MOCK_TS_2018_06_21,
    updated: MOCK_TS_2018_06_21,
  })

  const r2 = await dao.requireById('123')
  // console.log(r2)

  expect(r2).toEqual({
    id: '123',
    k1: 'k1',
    // k2: null, // no k2!
    created: MOCK_TS_2018_06_21,
    updated: MOCK_TS_2018_06_21,
  })
})
