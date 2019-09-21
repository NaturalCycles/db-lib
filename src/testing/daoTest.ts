import { _pick, _sortBy } from '@naturalcycles/js-lib'
import { toArray } from 'rxjs/operators'
import { CommonDao, CommonDaoLogLevel } from '../common.dao'
import { CommonDB } from '../common.db'
import { CommonDBTestOptions } from './dbTest'
import {
  createTestItemsBM,
  TEST_TABLE,
  testItemBMSchema,
  testItemDBMSchema,
  testItemTMSchema,
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

  const { allowQueryUnsorted, allowGetByIdsUnsorted, allowStreamQueryToBeUnsorted } = opt

  const items = createTestItemsBM(3)
  deepFreeze(items)
  const [item1] = items

  const expectedItems = items.map(i => ({
    ...i,
    updated: expect.any(Number),
  }))

  // DELETE ALL initially
  test('deleteByIds test items', async () => {
    const records = await dao
      .createQuery()
      .select([])
      .runQuery()
    await db.deleteByIds(TEST_TABLE, records.map(i => i.id))
  })

  // QUERY empty
  test('runQuery(all), runQueryCount should return empty', async () => {
    expect(await dao.createQuery().runQuery()).toEqual([])
    expect(await dao.createQuery().runQueryCount()).toEqual(0)
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
    expect(itemsSaved).toEqual(expectedItems)
  })

  // GET not empty
  test('getByIds all items', async () => {
    let records = await dao.getByIds(items.map(i => i.id).concat('abcd'))
    if (allowGetByIdsUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(expectedItems)
  })

  // QUERY
  test('runQuery(all) should return all items', async () => {
    let records = await dao.createQuery().runQuery()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(expectedItems)
  })

  test('query even=true', async () => {
    let records = await dao
      .createQuery('only even')
      .filter('even', '=', true)
      .runQuery()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(expectedItems.filter(i => i.even))
  })

  if (!allowQueryUnsorted) {
    test('query order by k1 desc', async () => {
      const records = await dao
        .createQuery('desc')
        .order('k1', true)
        .runQuery()
      expect(records).toEqual([...expectedItems].reverse())
    })
  }

  test('projection query with only ids', async () => {
    let records = await dao
      .createQuery()
      .select([])
      .runQuery()
    if (allowQueryUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(expectedItems.map(item => _pick(item, ['id'])))
  })

  test('runQueryCount should return 3', async () => {
    expect(await dao.createQuery().runQueryCount()).toBe(3)
  })

  // STREAM
  test('streamQuery all', async () => {
    let records = await dao
      .createQuery()
      .streamQuery()
      .pipe(toArray())
      .toPromise()

    if (allowStreamQueryToBeUnsorted) records = _sortBy(records, 'id')
    expect(records).toEqual(expectedItems)
  })

  // DELETE BY
  test('deleteByQuery even=false', async () => {
    const deleted = await dao
      .createQuery()
      .filter('even', '=', false)
      .deleteByQuery()
    expect(deleted).toBe(items.filter(item => !item.even).length)
    expect(await dao.createQuery().runQueryCount()).toBe(1)
  })

  test('cleanup', async () => {
    // CLEAN UP
    const records = await dao
      .createQuery()
      .select([])
      .runQuery()
    await db.deleteByIds(TEST_TABLE, records.map(i => i.id))
  })
}
