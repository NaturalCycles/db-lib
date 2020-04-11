import { filterObject, pDelay, pMap, _pick, _sortBy } from '@naturalcycles/js-lib'
import { streamMapToArray } from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import { DBQuery } from '../dbQuery'
import {
  createTestItemsDBM,
  getTestItemSchema,
  TestItemBM,
  TestItemDBM,
  TEST_TABLE,
} from './test.model'
import { deepFreeze } from './test.util'

export interface CommonDBImplementationFeatures {
  /**
   * All querying functionality.
   */
  querying?: boolean

  dbQueryFilter?: boolean
  dbQueryFilterIn?: boolean
  dbQueryOrder?: boolean
  dbQuerySelectFields?: boolean

  createTable?: boolean
  tableSchemas?: boolean

  /**
   * Queries should return fresh results immediately.
   * Datastore is the one known to NOT have strong consistency for queries (not for getById though).
   */
  strongConsistency?: boolean

  streaming?: boolean
}

/**
 * All options default to `false`.
 */
export interface CommonDBImplementationQuirks {
  /**
   * Applicable to e.g Datastore.
   * Time in milliseconds to wait for eventual consistency to propagate.
   */
  eventualConsistencyDelay?: number

  /**
   * Example: airtableId
   */
  allowExtraPropertiesInResponse?: boolean

  /**
   * Example: AirtableDB
   */
  allowBooleansAsUndefined?: boolean
}

/**
 * All unclaimed features will default to 'true'
 */
export function runCommonDBTest(
  db: CommonDB,
  features: CommonDBImplementationFeatures = {},
  quirks: CommonDBImplementationQuirks = {},
): void {
  const {
    querying = true,
    tableSchemas = true,
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

  const items = createTestItemsDBM(3)
  deepFreeze(items)
  const [item1] = items

  const queryAll = () => new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE)

  test('ping', async () => {
    await db.ping()
  })

  // CREATE TABLE, DROP
  if (createTable) {
    test('createTable, dropIfExists=true', async () => {
      await db.createTable(getTestItemSchema(), { dropIfExists: true })
    })
  }

  if (querying) {
    // DELETE ALL initially
    test('deleteByIds test items', async () => {
      const { records } = await db.runQuery(queryAll().select([]))
      await db.deleteByIds(
        TEST_TABLE,
        records.map(i => i.id),
      )
    })

    // QUERY empty
    test('runQuery(all), runQueryCount should return empty', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      expect((await db.runQuery(queryAll())).records).toEqual([])
      expect(await db.runQueryCount(queryAll())).toEqual(0)
    })
  }

  // GET empty
  test('getByIds(item1.id) should return empty', async () => {
    const [item1Loaded] = await db.getByIds<TestItemDBM>(TEST_TABLE, [item1.id])
    // console.log(a)
    expect(item1Loaded).toBeUndefined()
  })

  test('getByIds([]) should return []', async () => {
    expect(await db.getByIds(TEST_TABLE, [])).toEqual([])
  })

  test('getByIds(...) should return empty', async () => {
    expect(await db.getByIds(TEST_TABLE, ['abc', 'abcd'])).toEqual([])
  })

  // SAVE
  test('saveBatch test items', async () => {
    await db.saveBatch(TEST_TABLE, items)
  })

  // GET not empty
  test('getByIds all items', async () => {
    const records = await db.getByIds<TestItemDBM>(TEST_TABLE, items.map(i => i.id).concat('abcd'))
    expectMatch(items, records, quirks)
  })

  // QUERY
  if (querying) {
    test('runQuery(all) should return all items', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      let { records } = await db.runQuery(queryAll())
      records = _sortBy(records, 'id') // because query doesn't specify order here
      expectMatch(items, records, quirks)
    })

    if (dbQueryFilter) {
      test('query even=true', async () => {
        const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).filter('even', '=', true)
        let { records } = await db.runQuery(q)
        if (!dbQueryOrder) records = _sortBy(records, 'id')
        expectMatch(
          items.filter(i => i.even),
          records,
          quirks,
        )
      })
    }

    if (dbQueryOrder) {
      test('query order by k1 desc', async () => {
        const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).order('k1', true)
        const { records } = await db.runQuery(q)
        expectMatch([...items].reverse(), records, quirks)
      })
    }

    if (dbQuerySelectFields) {
      test('projection query with only ids', async () => {
        const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).select(['id'])
        let { records } = await db.runQuery(q)
        records = _sortBy(records, 'id') // cause order is not specified
        expectMatch(
          items.map(item => _pick(item, ['id'])),
          records,
          quirks,
        )
      })

      test('projection query without ids', async () => {
        const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).select(['k1'])
        let { records } = await db.runQuery(q)
        records = _sortBy(records, 'id') // cause order is not specified
        expectMatch(
          items.map(item => _pick(item, ['k1'])),
          records,
          quirks,
        )
      })

      test('projection query empty fields (edge case)', async () => {
        const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).select([])
        const { records } = await db.runQuery(q)
        expectMatch(
          items.map(() => ({})),
          records,
          quirks,
        )
      })
    }

    test('runQueryCount should return 3', async () => {
      expect(await db.runQueryCount(new DBQuery(TEST_TABLE))).toBe(3)
    })
  }

  // STREAM
  if (streaming) {
    test('streamQuery all', async () => {
      let records = await streamMapToArray(db.streamQuery(queryAll()))

      records = _sortBy(records, 'id') // cause order is not specified in DBQuery
      expectMatch(items, records, quirks)
    })
  }

  // getTables
  test('getTables, getTableSchema (if supported)', async () => {
    const tables = await db.getTables()
    console.log({ tables })

    if (tableSchemas) {
      await pMap(tables, async table => {
        const schema = await db.getTableSchema(table)
        console.log(schema)
        expect(schema).toBeDefined()
      })
    }
  })

  // DELETE BY
  if (querying && dbQueryFilter) {
    test('deleteByQuery even=false', async () => {
      const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).filter('even', '=', false)
      const deleted = await db.deleteByQuery(q)
      expect(deleted).toBe(items.filter(item => !item.even).length)

      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)

      expect(await db.runQueryCount(queryAll())).toBe(1)
    })
  }

  if (querying) {
    test('cleanup', async () => {
      // CLEAN UP
      const { records } = await db.runQuery(queryAll().select([]))
      await db.deleteByIds(
        TEST_TABLE,
        records.map(i => i.id),
      )
    })
  }
}

export function expectMatch(
  expected: any,
  actual: any,
  quirks: CommonDBImplementationQuirks,
): void {
  // const expectedSorted = sortObjectDeep(expected)
  // const actualSorted = sortObjectDeep(actual)

  if (quirks.allowBooleansAsUndefined) {
    expected = (Array.isArray(expected) ? expected : [expected]).map(r =>
      filterObject(r, (_k, v) => v !== false),
    )
  }

  if (quirks.allowExtraPropertiesInResponse) {
    expect(actual).toMatchObject(expected)
  } else {
    expect(actual).toEqual(expected)
  }
}
