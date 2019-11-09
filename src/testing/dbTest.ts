import { _pick, _sortBy, pDelay, pMap } from '@naturalcycles/js-lib'
import { streamMapToArray } from '@naturalcycles/nodejs-lib'
import { CommonDB } from '../common.db'
import { DBQuery } from '../dbQuery'
import {
  createTestItemsDBM,
  getTestItemSchema,
  TEST_TABLE,
  TestItemBM,
  TestItemDBM,
} from './test.model'
import { deepFreeze } from './test.util'

/**
 * All options default to `false`.
 */
export interface CommonDBTestOptions {
  allowQueryUnsorted?: boolean
  allowGetByIdsUnsorted?: boolean
  allowStreamQueryToBeUnsorted?: boolean

  /**
   * Applicable to e.g Datastore.
   * Time in milliseconds to wait for eventual consistency to propagate.
   */
  eventualConsistencyDelay?: number
}

export function runCommonDBTest(db: CommonDB, opt: CommonDBTestOptions = {}): void {
  const {
    allowQueryUnsorted,
    allowGetByIdsUnsorted,
    allowStreamQueryToBeUnsorted,
    eventualConsistencyDelay,
  } = opt

  const items = createTestItemsDBM(3)
  deepFreeze(items)
  const [item1] = items

  const queryAll = () => new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE, 'all')

  // CREATE TABLE, DROP
  test('createTable, dropIfExists=true', async () => {
    await db.createTable(getTestItemSchema(), { dropIfExists: true })
  })

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
    let records = await db.getByIds<TestItemDBM>(TEST_TABLE, items.map(i => i.id).concat('abcd'))
    if (allowGetByIdsUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(items)
  })

  // QUERY
  test('runQuery(all) should return all items', async () => {
    if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
    let { records } = await db.runQuery(queryAll())
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(items)
  })

  test('query even=true', async () => {
    const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE, 'only even').filter(
      'even',
      '=',
      true,
    )
    let { records } = await db.runQuery(q)
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(items.filter(i => i.even))
  })

  if (!allowQueryUnsorted) {
    test('query order by k1 desc', async () => {
      const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE, 'desc').order('k1', true)
      const { records } = await db.runQuery(q)
      expect(records).toEqual([...items].reverse())
    })
  }

  test('projection query with only ids', async () => {
    const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).select([])
    let { records } = await db.runQuery(q)
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(items.map(item => _pick(item, ['id'])))
  })

  test('runQueryCount should return 3', async () => {
    expect(await db.runQueryCount(new DBQuery(TEST_TABLE))).toBe(3)
  })

  // STREAM
  test('streamQuery all', async () => {
    let records = await streamMapToArray(db.streamQuery(queryAll()))

    if (allowStreamQueryToBeUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(items)
  })

  // getTables
  test('getTables, getTableSchema', async () => {
    const tables = await db.getTables()
    console.log({ tables })
    await pMap(tables, async table => {
      const schema = await db.getTableSchema(table)
      console.log(schema)
      expect(schema).toBeDefined()
    })
  })

  // DELETE BY
  test('deleteByQuery even=false', async () => {
    const q = new DBQuery<TestItemBM, TestItemDBM>(TEST_TABLE).filter('even', '=', false)
    const deleted = await db.deleteByQuery(q)
    expect(deleted).toBe(items.filter(item => !item.even).length)

    if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)

    expect(await db.runQueryCount(queryAll())).toBe(1)
  })

  test('cleanup', async () => {
    // CLEAN UP
    const { records } = await db.runQuery(queryAll().select([]))
    await db.deleteByIds(
      TEST_TABLE,
      records.map(i => i.id),
    )
  })
}
