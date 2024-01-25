import { mockTime, MOCK_TS_2018_06_21 } from '@naturalcycles/dev-lib/dist/testing'
import {
  ErrorMode,
  _omit,
  _range,
  _sortBy,
  pTry,
  pExpectedError,
  BaseDBEntity,
  _deepFreeze,
} from '@naturalcycles/js-lib'
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
  TEST_TABLE,
  TestItemBM,
  TestItemDBM,
  testItemBMJsonSchema,
  createTestItemBM,
} from '../testing'
import { CommonDao } from './common.dao'
import { CommonDaoCfg, CommonDaoLogLevel, CommonDaoSaveBatchOptions } from './common.dao.model'

let throwError = false

const db = new InMemoryDB()
const daoCfg: CommonDaoCfg<TestItemBM, TestItemDBM> = {
  table: TEST_TABLE,
  db,
  bmSchema: testItemBMSchema,
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

  expect(await dao.deleteById(undefined)).toBe(0)
  expect(await dao.deleteById('123')).toBe(0)
  expect(await dao.deleteByQuery(dao.query())).toBe(0)
  expect(await dao.deleteByQuery(dao.query(), { batchSize: 500 })).toBe(0)

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
  ).rejects.toThrowErrorMatchingInlineSnapshot(`"error_from_parseNaturalId"`)

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
  ).rejects.toThrowErrorMatchingInlineSnapshot(`"transformMap resulted in 1 error(s)"`)
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))

  // .stream should suppress by default
  results = []
  await _pipeline([dao.query().streamQuery(opt), writableForEach(r => void results.push(r))])
  expect(results).toEqual(items.filter(i => i.id !== 'id3'))
})

test('patchById', async () => {
  const id = '123456'
  const r = await dao.patchById(id, {
    k1: 'k111',
  })

  const r2 = await dao.getById(id)
  expect(r.id).toBe(id)
  expect(r2).toEqual(r)
  expect(r).toMatchInlineSnapshot(`
    {
      "created": 1529539200,
      "id": "123456",
      "k1": "k111",
      "updated": 1529539200,
    }
  `)
})

test('patch', async () => {
  const item: TestItemBM = await dao.save({
    id: 'id1',
    k1: 'k1',
  })

  // Something changes the item in a different process
  await dao.save({
    ...item,
    k1: 'k2',
  })

  // item.k1 is still the same in this process
  expect(item.k1).toBe('k1')

  // We want to set item.k3 to 1
  // Old-school careless way would be to just `item.k3 = 1` and save.
  // But that would overwrite the `k1 = k2` change above.
  // Instead, we apply a patch!
  // Then we inspect item, and it should reflect the `k1 = k2` change.
  const patchResult = await dao.patch(item, { k3: 5 })

  // It tracks the same object
  expect(patchResult).toBe(item)
  expect(item.k3).toBe(5) // patch is applied
  expect(item.k1).toBe('k2') // it pulled the change from DB (saved in the separate process)
  expect(item).toMatchInlineSnapshot(`
    {
      "created": 1529539200,
      "id": "id1",
      "k1": "k2",
      "k3": 5,
      "updated": 1529539200,
    }
  `)
})

// todo: fix jest mock
test.skip('ensureUniqueId', async () => {
  const opt: CommonDaoSaveBatchOptions<TestItemBM> = {
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
  const [item1, item2, item3] = createTestItemsBM(3).map(r => _omit(r, ['id']))
  const item1Saved = await immutableDao.save(item1!)

  item1Saved.k1 = 'modifiedk1'
  // Ensure object cannot be modified with save
  await expect(immutableDao.save(item1Saved)).rejects.toThrow()

  // Ensure objects be saved with saveBatch
  const bms = [item2!, item3!]

  await expect(immutableDao.saveBatch(bms)).resolves.not.toThrow()

  // Ensure Object can't be patched
  await expect(immutableDao.patchById(item1Saved.id, { k2: 'patchedk2' })).rejects.toThrow()

  // Ensure object can't be deleted
  await expect(immutableDao.deleteById(item1Saved.id)).rejects.toThrow()
  await expect(immutableDao.deleteByIds([item1Saved.id])).rejects.toThrow()
  const q = immutableDao.query().filter('id', '==', item1Saved.id)
  await expect(immutableDao.deleteByQuery(q)).rejects.toThrow()

  // Ensure deletion is possible with override flag
  await expect(immutableDao.deleteByQuery(q, { allowMutability: true })).resolves.not.toThrow()
  await expect(
    immutableDao.deleteById(item1Saved.id, { allowMutability: true }),
  ).resolves.not.toThrow()
  await expect(
    immutableDao.deleteByIds([item1Saved.id], { allowMutability: true }),
  ).resolves.not.toThrow()
})

test('ensure mutable objects can be written to multiple times', async () => {
  const [item1] = createTestItemsBM(1).map(r => _omit(r, ['id']))
  const item1Saved = await dao.save(item1!)

  item1Saved.k1 = 'modifiedk1'
  // Ensure object cannot be modified with save
  await expect(dao.save(item1Saved)).resolves.not.toThrow()
})

test('mutation', async () => {
  const obj = {
    id: '123',
    k1: 'k1',
    k2: null,
  }

  const saved = await dao.save(obj)

  // Should return the original object
  // eslint-disable-next-line jest/prefer-equality-matcher
  expect(obj === saved).toBe(true)

  // But `created`, `updated` should be "mutated" on the original object
  expect((obj as any).created).toBe(MOCK_TS_2018_06_21)
})

test('validateAndConvert does not mutate and returns new reference', async () => {
  const bm = createTestItemBM()
  _deepFreeze(bm)

  const bm2 = dao.validateAndConvert(bm, testItemBMSchema)
  // eslint-disable-next-line jest/prefer-equality-matcher
  expect(bm === bm2).toBe(false)
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
    AjvValidationError,
  )
  expect(err).toBeInstanceOf(AjvValidationError)
  expect(err).toMatchInlineSnapshot(`
[AjvValidationError: TEST_TABLE.id123/k1 must be string
Input: { id: 'id123', k1: 5, created: 1529539200, updated: 1529539200 }]
`)

  console.log((err as any).data)
})

interface Item extends BaseDBEntity {
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

test('runQuery stack', async () => {
  // save invalid value
  await dao.save(
    {
      id: 'invalid',
      even: true,
    } as TestItemBM,
    { skipValidation: true },
  )

  const err = await pExpectedError(getEven())
  expect(err.stack).toContain('at getEven (')
})

async function getEven(): Promise<TestItemBM[]> {
  return await dao.query().filterEq('even', true).runQuery()
}

test('runInTransaction', async () => {
  const items = createTestItemsBM(4)

  await dao.runInTransaction(async tx => {
    await tx.save(dao, items[0]!)
    await tx.save(dao, items[1]!)
    await tx.save(dao, items[3]!)
    await tx.deleteById(dao, items[1]!.id)
  })

  const items2 = await dao.query().runQuery()
  expect(items2.map(i => i.id).sort()).toEqual(['id1', 'id4'])
})
