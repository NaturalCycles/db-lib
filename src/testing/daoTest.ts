import { pDelay, _pick, _sortBy } from '@naturalcycles/js-lib'
import { streamMapToArray } from '@naturalcycles/nodejs-lib'
import { getTestItemSchema, TestItemBM } from '.'
import { ObjectWithId } from '..'
import { CommonDao, CommonDaoLogLevel } from '../common.dao'
import { CommonDB } from '../common.db'
import { CommonDBImplementationFeatures, CommonDBImplementationQuirks, expectMatch } from './dbTest'
import {
  createTestItemsBM,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
  TEST_TABLE,
} from './test.model'
import { deepFreeze } from './test.util'

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
  } = features

  const {
    // allowExtraPropertiesInResponse,
    // allowBooleansAsUndefined,
  } = quirks
  const eventualConsistencyDelay = !strongConsistency && quirks.eventualConsistencyDelay

  const items = createTestItemsBM(3)
  deepFreeze(items)
  const [item1] = items

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
      const records = await dao.query().select(['id']).runQuery<ObjectWithId>()
      await db.deleteByIds(
        TEST_TABLE,
        records.map(i => i.id),
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
    expect(await dao.getById(item1.id)).toBeUndefined()
  })

  test('getByIds([]) should return []', async () => {
    expect(await dao.getByIds([])).toEqual([])
  })

  test('getByIds(...) should return empty', async () => {
    expect(await dao.getByIds(['abc', 'abcd'])).toEqual([])
  })

  // SAVE
  test('saveBatch test items', async () => {
    const itemsSaved = await dao.saveBatch(items)
    expectMatch(expectedItems, itemsSaved, quirks)
  })

  // GET not empty
  test('getByIds all items', async () => {
    const records = await dao.getByIds(items.map(i => i.id).concat('abcd'))
    expectMatch(expectedItems, records, quirks)
  })

  // QUERY
  if (querying) {
    test('runQuery(all) should return all items', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      let records = await dao.query().runQuery()
      records = _sortBy(records, 'id')
      expectMatch(expectedItems, records, quirks)
    })

    if (dbQueryFilter) {
      test('query even=true', async () => {
        let records = await dao.query().filter('even', '=', true).runQuery()
        records = _sortBy(records, 'id')
        expectMatch(
          expectedItems.filter(i => i.even),
          records,
          quirks,
        )
      })
    }

    if (dbQueryOrder) {
      test('query order by k1 desc', async () => {
        const records = await dao.query().order('k1', true).runQuery()
        expectMatch([...expectedItems].reverse(), records, quirks)
      })
    }

    if (dbQuerySelectFields) {
      test('projection query with only ids', async () => {
        let records = await dao.query().select(['id']).runQuery<ObjectWithId>()
        records = _sortBy(records, 'id')
        expectMatch(
          expectedItems.map(item => _pick(item, ['id'])),
          records,
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
      let records: TestItemBM[] = []
      await dao.query().streamQueryForEach(bm => void records.push(bm))

      records = _sortBy(records, 'id')
      expectMatch(expectedItems, records, quirks)
    })

    test('streamQuery all', async () => {
      let records = await streamMapToArray(dao.query().streamQuery())

      records = _sortBy(records, 'id')
      expectMatch(expectedItems, records, quirks)
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
      let ids = await streamMapToArray(dao.query().streamQueryIds())
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
      const deleted = await dao.query().filter('even', '=', false).deleteByQuery()
      expect(deleted).toBe(items.filter(item => !item.even).length)
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      expect(await dao.query().runQueryCount()).toBe(1)
    })

    test('cleanup', async () => {
      // CLEAN UP
      const records = await dao.query().select(['id']).runQuery()
      await db.deleteByIds(
        TEST_TABLE,
        records.map(i => i.id),
      )
    })
  }
}
