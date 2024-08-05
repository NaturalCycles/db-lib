import { _range, _sortBy } from '@naturalcycles/js-lib'
import { CommonKeyValueDB, KeyValueDBTuple } from '../kv/commonKeyValueDB'
import { TEST_TABLE } from './test.model'

/* eslint-disable jest/no-export */

const testIds = _range(1, 4).map(n => `id${n}`)

const testEntries: KeyValueDBTuple[] = testIds.map(id => [id, Buffer.from(`${id}value`)])

export function runCommonKeyValueDBTest(db: CommonKeyValueDB): void {
  test('ping', async () => {
    await db.ping()
  })

  test('createTable', async () => {
    await db.createTable(TEST_TABLE, { dropIfExists: true })
  })

  test('deleteByIds non existing', async () => {
    await db.deleteByIds(TEST_TABLE, testIds)
  })

  test('getByIds should return empty', async () => {
    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toEqual([])
  })

  test('count should be 0', async () => {
    expect(await db.count(TEST_TABLE)).toBe(0)
  })

  test('saveBatch, then getByIds', async () => {
    await db.saveBatch(TEST_TABLE, testEntries)

    const entries = await db.getByIds(TEST_TABLE, testIds)
    _sortBy(entries, e => e[0], true)
    expect(entries).toEqual(testEntries)
  })

  test('count should be 3', async () => {
    expect(await db.count(TEST_TABLE)).toBe(3)
  })

  test('streamIds', async () => {
    const ids = await db.streamIds(TEST_TABLE).toArray()
    ids.sort()
    expect(ids).toEqual(testIds)
  })

  test('streamIds limited', async () => {
    const idsLimited = await db.streamIds(TEST_TABLE, 2).toArray()
    // Order is non-deterministic, so, cannot compare values
    // idsLimited.sort()
    // expect(idsLimited).toEqual(testIds.slice(0, 2))
    expect(idsLimited.length).toBe(2)
  })

  test('streamValues', async () => {
    const values = await db.streamValues(TEST_TABLE).toArray()
    values.sort()
    expect(values).toEqual(testEntries.map(e => e[1]))
  })

  test('streamValues limited', async () => {
    const valuesLimited = await db.streamValues(TEST_TABLE, 2).toArray()
    // valuesLimited.sort()
    // expect(valuesLimited).toEqual(testEntries.map(e => e[1]).slice(0, 2))
    expect(valuesLimited.length).toBe(2)
  })

  test('streamEntries', async () => {
    const entries = await db.streamEntries(TEST_TABLE).toArray()
    entries.sort()
    expect(entries).toEqual(testEntries)
  })

  test('streamEntries limited', async () => {
    const entriesLimited = await db.streamEntries(TEST_TABLE, 2).toArray()
    // entriesLimited.sort()
    // expect(entriesLimited).toEqual(testEntries.slice(0, 2))
    expect(entriesLimited.length).toBe(2)
  })

  test('deleteByIds should clear', async () => {
    await db.deleteByIds(TEST_TABLE, testIds)
    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toEqual([])
  })
}
