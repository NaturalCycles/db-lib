import { CommonKeyValueDB } from '../kv/commonKeyValueDB'
import { runCommonKeyValueDBTest } from './keyValueDBTest'

export const TEST_TABLE = 'TEST_TABLE'

export function runCommonHashKeyValueDBTest(db: CommonKeyValueDB): void {
  beforeAll(async () => {
    // Tests in this suite are not isolated,
    // and failing tests can leave the DB in an unexpected state for other tests,
    // including the following test run.
    // Here we clear the table before running the tests.
    const ids = await db.streamIds(TEST_TABLE).toArray()
    await db.deleteByIds(TEST_TABLE, ids)
  })

  afterAll(async () => {
    const ids = await db.streamIds(TEST_TABLE).toArray()
    await db.deleteByIds(TEST_TABLE, ids)
  })

  runCommonKeyValueDBTest(db)

  test('increment on a non-existing field should set the value to 1', async () => {
    const result = await db.increment(TEST_TABLE, 'nonExistingField')
    expect(result).toBe(1)
  })

  test('increment on a existing field should increase the value by one', async () => {
    const result = await db.increment(TEST_TABLE, 'nonExistingField')
    expect(result).toBe(2)
  })

  test('increment should increase the value by the specified amount', async () => {
    const result = await db.increment(TEST_TABLE, 'nonExistingField', 2)
    expect(result).toBe(4)
  })
}
