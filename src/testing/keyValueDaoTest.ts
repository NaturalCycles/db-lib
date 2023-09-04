import { _range, _sortBy } from '@naturalcycles/js-lib'
import { readableToArray } from '@naturalcycles/nodejs-lib'
import { KeyValueDBTuple } from '../kv/commonKeyValueDB'
import { CommonKeyValueDao } from '../kv/commonKeyValueDao'

const testIds = _range(1, 4).map(n => `id${n}`)
const testEntries: KeyValueDBTuple[] = testIds.map(id => [id, Buffer.from(`${id}value`)])

export function runCommonKeyValueDaoTest(dao: CommonKeyValueDao<Buffer>): void {
  test('ping', async () => {
    await dao.ping()
  })

  test('createTable', async () => {
    await dao.createTable({ dropIfExists: true })
  })

  test('deleteByIds non existing', async () => {
    await dao.deleteByIds(testIds)
  })

  test('getByIds should return empty', async () => {
    const results = await dao.getByIds(testIds)
    expect(results).toEqual([])
  })

  test('saveBatch, then getByIds', async () => {
    await dao.saveBatch(testEntries)

    const entries = await dao.getByIds(testIds)
    _sortBy(entries, e => e[0], true)
    expect(entries).toEqual(testEntries)
  })

  test('streamIds', async () => {
    const ids = await readableToArray(dao.streamIds())
    ids.sort()
    expect(ids).toEqual(testIds)
  })

  test('streamIds limited', async () => {
    const idsLimited = await readableToArray(dao.streamIds(2))
    // Order is non-deterministic, so, cannot compare values
    // idsLimited.sort()
    // expect(idsLimited).toEqual(testIds.slice(0, 2))
    expect(idsLimited.length).toBe(2)
  })

  test('streamValues', async () => {
    const values = await readableToArray(dao.streamValues())
    values.sort()
    expect(values).toEqual(testEntries.map(e => e[1]))
  })

  test('streamValues limited', async () => {
    const valuesLimited = await readableToArray(dao.streamValues(2))
    // valuesLimited.sort()
    // expect(valuesLimited).toEqual(testEntries.map(e => e[1]).slice(0, 2))
    expect(valuesLimited.length).toBe(2)
  })

  test('streamEntries', async () => {
    const entries = await readableToArray(dao.streamEntries())
    entries.sort()
    expect(entries).toEqual(testEntries)
  })

  test('streamEntries limited', async () => {
    const entriesLimited = await readableToArray(dao.streamEntries(2))
    // entriesLimited.sort()
    // expect(entriesLimited).toEqual(testEntries.slice(0, 2))
    expect(entriesLimited.length).toBe(2)
  })

  test('deleteByIds should clear', async () => {
    await dao.deleteByIds(testIds)
    const results = await dao.getByIds(testIds)
    expect(results).toEqual([])
  })
}
