import { pDelay, pMap, _filterObject, _pick, _sortBy } from '@naturalcycles/js-lib'
import { readableToArray } from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import { DBQuery } from '../query/dbQuery'
import {
  createTestItemDBM,
  createTestItemsDBM,
  getTestItemSchema,
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

  bufferSupport?: boolean
  nullValues?: boolean
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
    bufferSupport = true,
    nullValues = true,
  } = features

  // const {
  // allowExtraPropertiesInResponse,
  // allowBooleansAsUndefined,
  // } = quirks
  const eventualConsistencyDelay = !strongConsistency && quirks.eventualConsistencyDelay

  const items = createTestItemsDBM(3)
  deepFreeze(items)
  const item1 = items[0]!

  const queryAll = () => DBQuery.create<TestItemDBM>(TEST_TABLE)

  test('ping', async () => {
    await db.ping()
  })

  // CREATE TABLE, DROP
  if (createTable) {
    test('createTable, dropIfExists=true', async () => {
      await db.createTable(TEST_TABLE, getTestItemSchema(), { dropIfExists: true })
    })
  }

  if (querying) {
    // DELETE ALL initially
    test('deleteByIds test items', async () => {
      const { rows } = await db.runQuery(queryAll().select(['id']))
      await db.deleteByIds(
        TEST_TABLE,
        rows.map(i => i.id),
      )
    })

    // QUERY empty
    test('runQuery(all), runQueryCount should return empty', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      expect((await db.runQuery(queryAll())).rows).toEqual([])
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
  if (nullValues) {
    test('should allow to save and load null values', async () => {
      const item3 = {
        ...createTestItemDBM(3),
        k2: null,
      }
      deepFreeze(item3)
      await db.saveBatch(TEST_TABLE, [item3])
      const item3Loaded = (await db.getByIds<TestItemDBM>(TEST_TABLE, [item3.id]))[0]!
      expectMatch([item3], [item3Loaded], quirks)
      expect(item3Loaded.k2).toBe(null)
    })
  }

  test('undefined values should not be saved/loaded', async () => {
    const item3 = {
      ...createTestItemDBM(3),
      k2: undefined,
    }
    deepFreeze(item3)
    const expected = { ...item3 }
    delete expected.k2

    await db.saveBatch(TEST_TABLE, [item3])
    const item3Loaded = (await db.getByIds<TestItemDBM>(TEST_TABLE, [item3.id]))[0]!
    expectMatch([expected], [item3Loaded], quirks)
    expect(item3Loaded.k2).toBe(undefined)
    expect(Object.keys(item3Loaded)).not.toContain('k2')
  })

  test('saveBatch test items', async () => {
    await db.saveBatch(TEST_TABLE, items)
  })

  // GET not empty
  test('getByIds all items', async () => {
    const rows = await db.getByIds<TestItemDBM>(TEST_TABLE, items.map(i => i.id).concat('abcd'))
    expectMatch(items, rows, quirks)
  })

  // QUERY
  if (querying) {
    test('runQuery(all) should return all items', async () => {
      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
      let { rows } = await db.runQuery(queryAll())
      rows = _sortBy(rows, r => r.id) // because query doesn't specify order here
      expectMatch(items, rows, quirks)
    })

    if (dbQueryFilter) {
      test('query even=true', async () => {
        const q = new DBQuery<TestItemDBM>(TEST_TABLE).filter('even', '==', true)
        let { rows } = await db.runQuery(q)
        if (!dbQueryOrder) rows = _sortBy(rows, r => r.id)
        expectMatch(
          items.filter(i => i.even),
          rows,
          quirks,
        )
      })
    }

    if (dbQueryOrder) {
      test('query order by k1 desc', async () => {
        const q = new DBQuery<TestItemDBM>(TEST_TABLE).order('k1', true)
        const { rows } = await db.runQuery(q)
        expectMatch([...items].reverse(), rows, quirks)
      })
    }

    if (dbQuerySelectFields) {
      test('projection query with only ids', async () => {
        const q = new DBQuery<TestItemDBM>(TEST_TABLE).select(['id'])
        let { rows } = await db.runQuery(q)
        rows = _sortBy(rows, r => r.id) // cause order is not specified
        expectMatch(
          items.map(item => _pick(item, ['id'])),
          rows,
          quirks,
        )
      })

      test('projection query without ids', async () => {
        const q = new DBQuery<TestItemDBM>(TEST_TABLE).select(['k1'])
        let { rows } = await db.runQuery(q)
        rows = _sortBy(rows, r => r.k1) // cause order is not specified
        expectMatch(
          items.map(item => _pick(item, ['k1'])),
          rows,
          quirks,
        )
      })

      test('projection query empty fields (edge case)', async () => {
        const q = new DBQuery<TestItemDBM>(TEST_TABLE).select([])
        const { rows } = await db.runQuery(q)
        expectMatch(
          items.map(() => ({})),
          rows,
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
      let rows = await readableToArray(db.streamQuery(queryAll()))

      rows = _sortBy(rows, r => r.id) // cause order is not specified in DBQuery
      expectMatch(items, rows, quirks)
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
        expect(schema.$id).toBe(`${table}.schema.json`)
      })
    }
  })

  // DELETE BY
  if (querying && dbQueryFilter) {
    test('deleteByQuery even=false', async () => {
      const q = new DBQuery<TestItemDBM>(TEST_TABLE).filter('even', '==', false)
      const deleted = await db.deleteByQuery(q)
      expect(deleted).toBe(items.filter(item => !item.even).length)

      if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)

      expect(await db.runQueryCount(queryAll())).toBe(1)
    })
  }

  // BUFFER
  if (bufferSupport) {
    test('buffer support', async () => {
      const s = 'helloWorld 1'
      const b1 = Buffer.from(s)

      const item = {
        ...createTestItemDBM(1),
        b1,
      }
      await db.saveBatch(TEST_TABLE, [item])
      const [loaded] = await db.getByIds<TestItemDBM>(TEST_TABLE, [item.id])
      const b1Loaded = loaded!.b1!
      console.log({
        b11: typeof b1,
        b12: typeof b1Loaded,
        l1: b1.length,
        l2: b1Loaded.length,
        b1,
        b1Loaded,
      })
      expect(b1Loaded).toEqual(b1)
      expect(b1Loaded.toString()).toBe(s)
    })
  }

  if (querying) {
    test('cleanup', async () => {
      // CLEAN UP
      const { rows } = await db.runQuery(queryAll().select(['id']))
      await db.deleteByIds(
        TEST_TABLE,
        rows.map(i => i.id),
      )
    })
  }
}

export function expectMatch(
  expected: any[],
  actual: any[],
  quirks: CommonDBImplementationQuirks,
): void {
  // const expectedSorted = sortObjectDeep(expected)
  // const actualSorted = sortObjectDeep(actual)

  if (quirks.allowBooleansAsUndefined) {
    expected = expected.map(r => {
      return typeof r !== 'object' ? r : _filterObject(r, (_k, v) => v !== false)
    })
  }

  if (quirks.allowExtraPropertiesInResponse) {
    expect(actual).toMatchObject(expected)
  } else {
    expect(actual).toEqual(expected)
  }
}
