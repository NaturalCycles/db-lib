import { StringMap, _range } from '@naturalcycles/js-lib'
import { CommonKVDao } from '../kv/common.kv.dao'

const testIds = _range(1, 4).map(n => `id${n}`)

// 'id1' => Buffer('id1value')
const testItems: StringMap<Buffer> = Object.fromEntries(
  testIds.map(id => [id, Buffer.from(`${id}value`)]),
)

export function runCommonKVDaoTest(dao: CommonKVDao<Buffer>): void {
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
    await dao.saveBatch(testItems)

    const results = await dao.getByIds(testIds)
    expect(results).toEqual(Object.values(testItems))
  })

  test('deleteByIds should clear', async () => {
    await dao.deleteByIds(testIds)
    const results = await dao.getByIds(testIds)
    expect(results).toEqual([])
  })
}
