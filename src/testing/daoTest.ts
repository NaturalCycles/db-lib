import { pDelay, _pick, _sortBy } from '@naturalcycles/js-lib'
import { streamMapToArray } from '@naturalcycles/nodejs-lib'
import { CommonDao, CommonDaoLogLevel } from '../common.dao'
import { CommonDB } from '../common.db'
import { getTestItemSchema, ObjectWithId, TestItemBM } from '../index'
import { CommonDBTestOptions, expectMatch } from './dbTest'
import {
  createTestItemsBM,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
  TEST_TABLE,
} from './test.model'
import { deepFreeze } from './test.util'

export function runCommonDaoTest(db: CommonDB, opt: CommonDBTestOptions = {}): void {
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
    allowQueryUnsorted,
    allowGetByIdsUnsorted,
    allowStreamQueryToBeUnsorted,
    eventualConsistencyDelay,
  } = opt

  const items = createTestItemsBM(3)
  deepFreeze(items)
  const [item1] = items

  const expectedItems = items.map(i => ({
    ...i,
    updated: expect.any(Number),
  }))

  // CREATE TABLE, DROP
  test('createTable, dropIfExists=true', async () => {
    await dao.createTable(getTestItemSchema(), { dropIfExists: true })
  })

  // DELETE ALL initially
  test('deleteByIds test items', async () => {
    const records = await dao
      .query()
      .select([])
      .runQuery<ObjectWithId>()
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
    expectMatch(expectedItems, itemsSaved, opt)
  })

  // GET not empty
  test('getByIds all items', async () => {
    let records = await dao.getByIds(items.map(i => i.id).concat('abcd'))
    if (allowGetByIdsUnsorted) records = _sortBy(records, 'id')
    expectMatch(expectedItems, records, opt)
  })

  // QUERY
  test('runQuery(all) should return all items', async () => {
    if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
    let records = await dao.query().runQuery()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expectMatch(expectedItems, records, opt)
  })

  test('query even=true', async () => {
    let records = await dao
      .query()
      .filter('even', '=', true)
      .runQuery()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expectMatch(
      expectedItems.filter(i => i.even),
      records,
      opt,
    )
  })

  if (!allowQueryUnsorted) {
    test('query order by k1 desc', async () => {
      const records = await dao
        .query()
        .order('k1', true)
        .runQuery()
      expectMatch([...expectedItems].reverse(), records, opt)
    })
  }

  test('projection query with only ids', async () => {
    let records = await dao
      .query()
      .select([])
      .runQuery<ObjectWithId>()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expectMatch(
      expectedItems.map(item => _pick(item, ['id'])),
      records,
      opt,
    )
  })

  test('runQueryCount should return 3', async () => {
    expect(await dao.query().runQueryCount()).toBe(3)
  })

  // STREAM
  test('streamQueryForEach all', async () => {
    let records: TestItemBM[] = []
    await dao.query().streamQueryForEach(bm => void records.push(bm))

    if (allowStreamQueryToBeUnsorted) records = _sortBy(records, 'id')
    expectMatch(expectedItems, records, opt)
  })

  test('streamQuery all', async () => {
    let records = await streamMapToArray(dao.query().streamQuery())

    if (allowStreamQueryToBeUnsorted) records = _sortBy(records, 'id')
    expectMatch(expectedItems, records, opt)
  })

  test('streamQueryIdsForEach all', async () => {
    let ids: string[] = []
    await dao.query().streamQueryIdsForEach(id => void ids.push(id))

    if (allowStreamQueryToBeUnsorted) ids = ids.sort()
    expectMatch(
      expectedItems.map(i => i.id),
      ids,
      opt,
    )
  })

  test('streamQueryIds all', async () => {
    let ids = await streamMapToArray(dao.query().streamQueryIds())

    if (allowStreamQueryToBeUnsorted) ids = ids.sort()
    expectMatch(
      expectedItems.map(i => i.id),
      ids,
      opt,
    )
  })

  // DELETE BY
  test('deleteByQuery even=false', async () => {
    const deleted = await dao
      .query()
      .filter('even', '=', false)
      .deleteByQuery()
    expect(deleted).toBe(items.filter(item => !item.even).length)
    if (eventualConsistencyDelay) await pDelay(eventualConsistencyDelay)
    expect(await dao.query().runQueryCount()).toBe(1)
  })

  test('cleanup', async () => {
    // CLEAN UP
    const records = await dao
      .query()
      .select([])
      .runQuery()
    await db.deleteByIds(
      TEST_TABLE,
      records.map(i => i.id),
    )
  })
}
