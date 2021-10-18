import { pDelay, _deepCopy, _pick, _sortBy } from '@naturalcycles/js-lib'
import { readableToArray, transformNoOp } from '@naturalcycles/nodejs-lib'
import { CommonDaoLogLevel } from '..'
import { CommonDB } from '../common.db'
import { CommonDao } from '../commondao/common.dao'
import { CommonDBImplementationFeatures, CommonDBImplementationQuirks, expectMatch } from './dbTest'
import {
  createTestItemsBM,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
  TEST_TABLE,
  createTestItemBM,
} from './test.model'
import { getTestItemSchema, TestItemBM } from '.'

export function runCommonDaoTest(
  db: CommonDB,
  features: CommonDBImplementationFeatures = {},
  quirks: CommonDBImplementationQuirks = {},
): void {
  const dao = new CommonDao({
    table: TEST_TABLE,
    db,
    dbmSchema: testItemDBMSchema,
    bmSchema: testItemBMSchema,
    tmSchema: testItemTMSchema,
    logStarted: true,
    logLevel: CommonDaoLogLevel.DATA_FULL,
  })

  const {
    querying = true,
    // tableSchemas = true,
    createTable = true,
    dbQueryFilter = true,
    // dbQueryFilterIn = true,
    dbQueryOrder = true,
    dbQuerySelectFields = true,
    streaming = true,
    strongConsistency = true,
    nullValues = true,
  } = features

  // const {
  // allowExtraPropertiesInResponse,
  // allowBooleansAsUndefined,
  // } = quirks
  const eventualConsistencyDelay = !strongConsistency && quirks.eventualConsistencyDelay

  const items = createTestItemsBM(3)
  const itemsClone = _deepCopy(items)
  // deepFreeze(items) // mutation of id/created/updated is allowed now! (even expected)
  const item1 = items[0]!

  const expectedItems = items.map(i => ({
    ...i,
    updated: expect.any(Number),
  }))

  test('ping', async () => {
    await dao.ping()
  })

  // CREATE TABLE, DROP
  if (createTable) {
    test('createTable, dropIfExists=true', async () => {
      await dao.createTable(getTestItemSchema(), { dropIfExists: true })
    })
  }

  if (querying) {
    // DELETE ALL initially
    test('deleteByIds test items', async () => {
      const rows = await dao.query().select(['id']).runQuery()
      await db.deleteByIds(
        TEST_TABLE,
        rows.map(i => i.id),
      )
    })

    // QUERY empty
    test('runQuery(all), runQueryCount should return empty', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      expect(await dao.query().runQuery()).toEqual([])
      expect(await dao.query().runQueryCount()).toEqual(0)
    })
  }

  // GET empty
  test('getByIds(item1.id) should return empty', async () => {
    const [item1Loaded] = await dao.getByIds([item1.id])
    expect(item1Loaded).toBeUndefined()
    expect(await dao.getById(item1.id)).toBeNull()
  })

  test('getByIds([]) should return []', async () => {
    expect(await dao.getByIds([])).toEqual([])
  })

  test('getByIds(...) should return empty', async () => {
    expect(await dao.getByIds(['abc', 'abcd'])).toEqual([])
  })

  // SAVE
  if (nullValues) {
    test('should allow to save and load null values', async () => {
      const item3 = {
        ...createTestItemBM(3),
        k2: null,
      }
      // deepFreeze(item3) // no, Dao is expected to mutate object to add id, created, updated
      await dao.save(item3)
      const item3Loaded = await dao.requireById(item3.id)
      expectMatch([item3], [item3Loaded], quirks)
      expect(item3Loaded.k2).toBe(null)
      expect(Object.keys(item3)).toContain('k2')
      expect(item3.k2).toBeNull()
    })
  }

  test('undefined values should not be saved/loaded', async () => {
    const item3 = {
      ...createTestItemBM(3),
      k2: undefined,
    }
    // deepFreeze(item3) // no, Dao is expected to mutate object to add id, created, updated
    const expected = { ...item3 }
    delete expected.k2

    await dao.save(item3)

    expected.updated = item3.updated // as it's mutated

    const item3Loaded = await dao.requireById(item3.id)
    expectMatch([expected], [item3Loaded], quirks)
    expect(item3Loaded.k2).toBe(undefined)
    expect(Object.keys(item3Loaded)).not.toContain('k2')
    expect(Object.keys(item3)).toContain('k2')
    expect(item3.k2).toBeUndefined()
  })

  test('saveBatch test items', async () => {
    const itemsSaved = await dao.saveBatch(items)
    expect(itemsSaved[0]).toBe(items[0]) // expect "same object" returned

    // no unnecessary mutation
    const { updated: _, ...clone } = itemsClone[0]!
    expect(items[0]).toMatchObject(clone)

    expectMatch(expectedItems, itemsSaved, quirks)
  })

  // GET not empty
  test('getByIds all items', async () => {
    const rows = await dao.getByIds(items.map(i => i.id).concat('abcd'))
    expectMatch(expectedItems, rows, quirks)
  })

  // QUERY
  if (querying) {
    test('runQuery(all) should return all items', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      let rows = await dao.query().runQuery()
      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    if (dbQueryFilter) {
      test('query even=true', async () => {
        let rows = await dao.query().filter('even', '==', true).runQuery()
        rows = _sortBy(rows, r => r.id)
        expectMatch(
          expectedItems.filter(i => i.even),
          rows,
          quirks,
        )
      })
    }

    if (dbQueryOrder) {
      test('query order by k1 desc', async () => {
        const rows = await dao.query().order('k1', true).runQuery()
        expectMatch([...expectedItems].reverse(), rows, quirks)
      })
    }

    if (dbQuerySelectFields) {
      test('projection query with only ids', async () => {
        let rows = await dao.query().select(['id']).runQuery()
        rows = _sortBy(rows, r => r.id)
        expectMatch(
          expectedItems.map(item => _pick(item, ['id'])),
          rows,
          quirks,
        )
      })
    }

    test('runQueryCount should return 3', async () => {
      expect(await dao.query().runQueryCount()).toBe(3)
    })
  }

  // STREAM
  if (streaming) {
    test('streamQueryForEach all', async () => {
      let rows: TestItemBM[] = []
      await dao.query().streamQueryForEach(bm => void rows.push(bm))

      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    test('streamQuery all', async () => {
      // let rows = await readableToArray(dao.query().streamQuery())
      // todo: remove transformNoOp after `transformMap` learns to be async-iteration-friendly
      let rows: TestItemBM[] = await readableToArray(
        dao.query().streamQuery().pipe(transformNoOp()),
      )

      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    test('streamQueryIdsForEach all', async () => {
      let ids: string[] = []
      await dao.query().streamQueryIdsForEach(id => void ids.push(id))
      ids = ids.sort()
      expectMatch(
        expectedItems.map(i => i.id),
        ids,
        quirks,
      )
    })

    test('streamQueryIds all', async () => {
      let ids = await readableToArray(dao.query().streamQueryIds())
      ids = ids.sort()
      expectMatch(
        expectedItems.map(i => i.id),
        ids,
        quirks,
      )
    })
  }

  // DELETE BY
  if (querying) {
    test('deleteByQuery even=false', async () => {
      const deleted = await dao.query().filter('even', '==', false).deleteByQuery()
      expect(deleted).toBe(items.filter(item => !item.even).length)
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      expect(await dao.query().runQueryCount()).toBe(1)
    })

    test('cleanup', async () => {
      // CLEAN UP
      const rows = await dao.query().select(['id']).runQuery()
      await db.deleteByIds(
        TEST_TABLE,
        rows.map(i => i.id),
      )
    })
  }
}
