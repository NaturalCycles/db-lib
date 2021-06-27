import { StringMap, _range } from '@naturalcycles/js-lib'
import { CommonKVDB } from '../kv/common.kv.db'
import { TEST_TABLE } from './test.model'

const testIds = _range(1, 4).map(n => `id${n}`)

// 'id1' => Buffer('id1value')
const testItems: StringMap<Buffer> = Object.fromEntries(
  testIds.map(id => [id, Buffer.from(`${id}value`)]),
)

export function runCommonKVDBTest(db: CommonKVDB): void {
  test('ping', async () => {
    await db.ping()
  })

  test('deleteByIds non existing', async () => {
    await db.deleteByIds(TEST_TABLE, testIds)
  })

  test('getByIds should return empty', async () => {
    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toEqual([])
  })

  test('saveBatch, then getByIds', async () => {
    await db.saveBatch(TEST_TABLE, testItems)

    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toStrictEqual(Object.values(testItems))
  })

  test('deleteByIds should clear', async () => {
    await db.deleteByIds(TEST_TABLE, testIds)
    const results = await db.getByIds(TEST_TABLE, testIds)
    expect(results).toEqual([])
  })
}
