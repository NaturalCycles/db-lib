import { mockTime, MOCK_TS_2018_06_21 } from '@naturalcycles/dev-lib/dist/testing'
import { ErrorMode, _omit, _range, _sortBy, pTry } from '@naturalcycles/js-lib'
import {
  AjvSchema,
  AjvValidationError,
  writableForEach,
  _pipeline,
  deflateString,
  inflateToString,
} from '@naturalcycles/nodejs-lib'
import { InMemoryDB } from '../adapter/inmemory/inMemory.db'
import { DBLibError } from '../cnst'
import {
  createTestItemsBM,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
  TEST_TABLE,
  TestItemDBM,
  TestItemBM,
} from '../testing'
import { testItemBMJsonSchema, testItemDBMJsonSchema } from '../testing/test.model'
import { DBQuery } from '../query/dbQuery'
import { CommonDao } from './common.dao'
import { CommonDaoCfg, CommonDaoLogLevel, CommonDaoSaveOptions } from './common.dao.model'

let throwError = false

const db = new InMemoryDB()
const daoCfg: CommonDaoCfg<TestItemBM> = {
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
}
const dao = new CommonDao(daoCfg)

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
  expect(await dao.deleteByQuery(dao.query())).toBe(0)
  expect(await dao.deleteByQuery(dao.query(), { stream: true })).toBe(0)

  expect(dao.anyToDBM(undefined)).toBeUndefined()
  expect(dao.anyToDBM({}, { skipValidation: true })).toMatchObject({})
})

test('runUnionQuery', async () => {
  const items = createTestItemsBM(5)
  await dao.saveBatch(items)

  const items2 = await dao.runUnionQueries([
    dao.query().filterEq('even', true),
    dao.query().filterEq('even', false),
    dao.query().filterEq('even', false), // again, to test uniqueness
  ])

  expect(_sortBy(items2, r => r.id)).toEqual(items)
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
  await dao.query().streamQueryForEach(r => void results.push(r), {
    ...opt,
    errorMode: ErrorMode.SUPPRESS,
  })
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))

  // THROW_IMMEDIATELY
  const results2 = []
  await expect(
    dao.query().streamQueryForEach(r => void results2.push(r), {
      ...opt,
      errorMode: ErrorMode.THROW_IMMEDIATELY,
    }),
  ).rejects.toThrow('error_from_parseNaturalId')

  // Throws on 3rd element, all previous elements should be collected
  // Cannot expect it cause with async dbmToBM it uses async `transformMap`, so
  // the execution is not sequential
  // expect(results2).toEqual(items.slice(0, 2))

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

  // timeout is not important here, adding to test that code path
  const r2 = await dao.getById(id, { timeout: 1000 })

  expect(r.id).toBe(id)
  expect(r2).toEqual(r)
  expect(r).toMatchSnapshot()
})

// todo: fix jest mock
test.skip('ensureUniqueId', async () => {
  const opt: CommonDaoSaveOptions<TestItemDBM> = {
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

test('modifications of immutable objects', async () => {
  const immutableDao = new CommonDao({ ...daoCfg, immutable: true })

  // Will be autogenerated (both items)
  const [item1] = createTestItemsBM(1).map(r => _omit(r, ['id']))
  const item1Saved = await immutableDao.save(item1!)

  item1Saved.k1 = 'modifiedk1'
  // Ensure object cannot be modified with save
  await expect(immutableDao.save(item1Saved)).rejects.toThrow()

  // Ensure object cannot be modified with saveBatch
  await expect(immutableDao.saveBatch([item1Saved])).rejects.toThrow()

  // Ensure Object can't be patched
  await expect(immutableDao.patch(item1Saved.id, { k2: 'patchedk2' })).rejects.toThrow()

  // Ensure object can't be deleted
  await expect(immutableDao.deleteById(item1Saved.id)).rejects.toThrow()
  await expect(immutableDao.deleteByIds([item1Saved.id])).rejects.toThrow()
  const q = new DBQuery('TestKind').filter('id', '==', item1Saved.id)
  await expect(immutableDao.deleteByQuery(q)).rejects.toThrow()

  await expect(immutableDao.deleteByQuery(q, { overrideImmutability: true })).resolves.not.toThrow()
})

test('mutation', async () => {
  const obj = {
    id: '123',
    k1: 'k1',
    k2: null,
  }

  const saved = await dao.save(obj)

  // Should be a new object, not the same (by reference)
  // Non-mutation should only be ensured inside `validateAndConvert` method
  // NO: should return the original object
  expect(obj === saved).toBe(true)

  // But `created`, `updated` should be "mutated" on the original object
  expect((obj as any).created).toBe(MOCK_TS_2018_06_21)
})

test('should preserve null on load and save', async () => {
  const r = await dao.save({
    id: '123',
    k1: 'k1',
    k2: null,
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
    k2: null,
    created: MOCK_TS_2018_06_21,
    updated: MOCK_TS_2018_06_21,
  })
})

test('does not reset updated on getByIdAsDBM', async () => {
  const r = await dao.save({
    id: '123',
    k1: 'k1',
    k2: null,
  })
  const updated1 = r.updated
  // console.log(r.updated)

  // 5 seconds later
  const newNow = MOCK_TS_2018_06_21 + 5000
  mockTime(newNow)

  const bm = await dao.requireById(r.id)
  // console.log(bm.updated)
  expect(bm.updated).toBe(updated1) // unchanged

  const dbm = await dao.requireByIdAsDBM(r.id)
  // console.log(bm.updated)
  expect(dbm.updated).toBe(updated1) // unchanged

  const r2 = await dao.save(r)
  expect(r2.created).toBe(updated1)
  expect(r2.updated).toBe(newNow) // updated!

  const [r2b] = await dao.saveBatch([r])
  expect(r2b!.created).toBe(updated1)
  expect(r2b!.updated).toBe(newNow) // updated!

  const r3 = await dao.saveAsDBM(r)
  expect(r3.created).toBe(updated1)
  expect(r3.updated).toBe(newNow) // updated!

  const [r3b] = await dao.saveBatchAsDBM([r])
  expect(r3b!.created).toBe(updated1)
  expect(r3b!.updated).toBe(newNow) // updated!
})

test('ajvSchema', async () => {
  const dao = new CommonDao({
    table: TEST_TABLE,
    db,
    bmSchema: AjvSchema.create(testItemBMJsonSchema),
    dbmSchema: AjvSchema.create(testItemDBMJsonSchema),
  })

  const items = createTestItemsBM(3)

  // Should pass validation
  await dao.saveBatch(items)
  await dao.save({
    k1: 'sdf',
  })

  // This should fail
  const [err] = await pTry(
    dao.save({
      id: 'id123', // provided, so we can snapshot-match
      k1: 5 as any,
    }),
  )
  expect(err).toBeInstanceOf(AjvValidationError)
  expect(err).toMatchInlineSnapshot(`
    [AjvValidationError: TEST_TABLEDBM.id123/k1 must be string
    Input: { id: 'id123', k1: 5, created: 1529539200, updated: 1529539200 }]
  `)

  console.log((err as any).data)
})

interface Item {
  id: string
  obj: any
}

test('zipping/unzipping via async hook', async () => {
  const dao = new CommonDao<Item>({
    table: TEST_TABLE,
    db,
    hooks: {
      async beforeBMToDBM(bm) {
        return {
          ...bm,
          obj: await deflateString(JSON.stringify(bm.obj)),
        }
      },
      async beforeDBMToBM(dbm) {
        return {
          ...dbm,
          obj: JSON.parse(await inflateToString(dbm.obj)),
        }
      },
    },
  })

  const items = _range(3).map(n => ({
    id: `id${n}`,
    obj: {
      objId: `objId${n}`,
    },
  }))

  await dao.saveBatch(items)

  const items2 = await dao.getByIds(items.map(item => item.id))
  expect(items2).toEqual(items)
})
