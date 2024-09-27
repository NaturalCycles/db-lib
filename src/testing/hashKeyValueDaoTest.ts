import { CommonKeyValueDao } from '../kv/commonKeyValueDao'
import { runCommonKeyValueDaoTest } from './keyValueDaoTest'

export function runCommonHashKeyValueDaoTest(dao: CommonKeyValueDao<Buffer>): void {
  beforeAll(async () => {
    // Tests in this suite are not isolated,
    // and failing tests can leave the DB in an unexpected state for other tests,
    // including the following test run.
    // Here we clear the table before running the tests.
    const ids = await dao.streamIds().toArray()
    await dao.deleteByIds(ids)
  })

  afterAll(async () => {
    const ids = await dao.streamIds().toArray()
    await dao.deleteByIds(ids)
  })

  runCommonKeyValueDaoTest(dao)

  test('increment on a non-existing field should set the value to 1', async () => {
    const result = await dao.increment('nonExistingField')
    expect(result).toBe(1)
  })

  test('increment on a existing field should increase the value by one', async () => {
    const result = await dao.increment('nonExistingField')
    expect(result).toBe(2)
  })

  test('increment should increase the value by the specified amount', async () => {
    const result = await dao.increment('nonExistingField', 2)
    expect(result).toBe(4)
  })
}
