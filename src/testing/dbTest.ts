import { _pick, _sortBy } from '@naturalcycles/js-lib'
import { toArray } from 'rxjs/operators'
import { CommonDB } from '../common.db'
import { DBQuery } from '../dbQuery'
import { createTestItems, TEST_TABLE, TestItem } from './test.model'
import { deepFreeze } from './test.util'

/**
 * All options default to `false`.
 */
export interface CommonDBTestOptions {
  allowGetByIdsUnsorted?: boolean
  allowStreamQueryToBeUnsorted?: boolean
}

export async function runCommonDBTest (db: CommonDB, opt: CommonDBTestOptions = {}): Promise<void> {
  const { allowGetByIdsUnsorted, allowStreamQueryToBeUnsorted } = opt

  const items = createTestItems(3)
  deepFreeze(items)
  const [item1] = items

  const queryAll = () => new DBQuery<TestItem>(TEST_TABLE, 'all')

  // DELETE ALL initially
  let itemsLoaded = await db.runQuery(queryAll().select([]))
  await db.deleteByIds(TEST_TABLE, itemsLoaded.map(i => i.id))

  // QUERY empty

  expect(await db.runQuery(queryAll())).toEqual([])
  expect(await db.runQueryCount(queryAll())).toEqual(0)

  // GET empty

  const [item1Loaded] = await db.getByIds<TestItem>(TEST_TABLE, [item1.id])
  // console.log(a)
  expect(item1Loaded).toBeUndefined()

  expect(await db.getByIds(TEST_TABLE, [])).toEqual([])
  expect(await db.getByIds(TEST_TABLE, ['abc', 'abcd'])).toEqual([])

  // SAVE

  await db.saveBatch<TestItem>(TEST_TABLE, items)

  // GET not empty

  itemsLoaded = await db.getByIds<TestItem>(TEST_TABLE, items.map(i => i.id).concat('abcd'))

  if (allowGetByIdsUnsorted) {
    expect(_sortBy(itemsLoaded, 'id')).toEqual(items)
  } else {
    expect(itemsLoaded).toEqual(items)
  }

  // QUERY
  itemsLoaded = await db.runQuery(queryAll())
  expect(_sortBy(itemsLoaded, 'id')).toEqual(items)
  // console.log(itemsLoaded)

  let q = new DBQuery<TestItem>(TEST_TABLE, 'only even').filter('even', '=', true)
  itemsLoaded = await db.runQuery(q)
  expect(_sortBy(itemsLoaded, 'id')).toEqual(items.filter(i => i.even))

  q = new DBQuery<TestItem>(TEST_TABLE, 'desc').order('k1', true)
  itemsLoaded = await db.runQuery(q)
  expect(itemsLoaded).toEqual([...items].reverse())

  q = new DBQuery<TestItem>(TEST_TABLE).select([])
  itemsLoaded = await db.runQuery(q)
  expect(_sortBy(itemsLoaded, 'id')).toEqual(items.map(item => _pick(item, ['id'])))

  expect(await db.runQueryCount(new DBQuery(TEST_TABLE))).toBe(3)

  // STREAM
  itemsLoaded = await db
    .streamQuery(queryAll())
    .pipe(toArray())
    .toPromise()

  if (allowStreamQueryToBeUnsorted) {
    expect(itemsLoaded).toEqual(items)
  } else {
    expect(_sortBy(itemsLoaded, 'id')).toEqual(items)
  }

  // DELETE BY
  q = new DBQuery<TestItem>(TEST_TABLE).filter('even', '=', false)
  const deleted = await db.deleteByQuery(q)
  expect(deleted).toBe(items.filter(item => !item.even).length)

  expect(await db.runQueryCount(queryAll())).toBe(1)

  // CLEAN UP
  itemsLoaded = await db.runQuery(queryAll().select([]))
  await db.deleteByIds(TEST_TABLE, itemsLoaded.map(i => i.id))
}
