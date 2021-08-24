import { _range, _sortBy } from '@naturalcycles/js-lib'
import { readableToArray } from '@naturalcycles/nodejs-lib'
import { CommonKeyValueDB, KeyValueDBTuple } from '../kv/commonKeyValueDB'
import { TEST_TABLE } from './test.model'

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

  test('saveBatch, then getByIds', async () => {
    await db.saveBatch(TEST_TABLE, testEntries)

    const entries = await db.getByIds(TEST_TABLE, testIds)
    _sortBy(entries, e => e[0], true)
    expect(entries).toEqual(testEntries)
  })

  test('streamIds', async () => {
    const ids = await readableToArray(db.streamIds(TEST_TABLE))
    ids.sort()
    expect(ids).toEqual(testIds)
  })

  test('streamIds limited', async () => {
    const idsLimited = await readableToArray(db.streamIds(TEST_TABLE, 2))
    idsLimited.sort()
    expect(idsLimited).toEqual(testIds.slice(0, 2))
  })

  test('streamValues', async () => {
    const values = await readableToArray(db.streamValues(TEST_TABLE))
    values.sort()
    expect(values).toEqual(testEntries.map(e => e[1]))
  })

  test('streamValues limited', async () => {
    const valuesLimited = await readableToArray(db.streamValues(TEST_TABLE, 2))
    valuesLimited.sort()
    expect(valuesLimited).toEqual(testEntries.map(e => e[1]).slice(0, 2))
  })

  test('streamEntries', async () => {
    const entries = await readableToArray(db.streamEntries(TEST_TABLE))
    entries.sort()
    expect(entries).toEqual(testEntries)
  })

  test('streamEntries limited', async () => {
    const entriesLimited = await readableToArray(db.streamEntries(TEST_TABLE, 2))
    entriesLimited.sort()
    expect(entriesLimited).toEqual(testEntries.slice(0, 2))
  })

  test('deleteByIds should clear', async () => {
    await db.deleteByIds(TEST_TABLE, testIds)
    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toEqual([])
  })
}
