import { loadSecretsFromEnv } from '@naturalcycles/nodejs-lib'
import { getDB } from './getDB'
import { InMemoryDB } from './index'

test('should throw if DB1 not defined', async () => {
  expect(() => getDB()).toThrow('DB1')
})

test('should throw if DB1 points to unknown lib', async () => {
  process.env.DB1 = 'nonexistinglib'
  expect(() => getDB()).toThrow('nonexistinglib')
})

test('getDBAdapter not implemented', async () => {
  loadSecretsFromEnv()
  process.env.DB1 = `${process.cwd()}`
  expect(() => getDB()).toThrow('not defined')
})

test('getDB InMemory', async () => {
  loadSecretsFromEnv()
  process.env.DB1 = `${process.cwd()}/src/adapter/inmemory`
  process.env.DB2 = process.env.DB1
  const db0 = getDB()
  expect(db0).toBeInstanceOf(InMemoryDB)

  const db1 = getDB(1)
  const db11 = getDB(1)
  const db2 = getDB(2)
  expect(db0).toBe(db1) // same reference due to 1 being default
  expect(db1).toBe(db11) // memoization should return same instance
  expect(db1).not.toBe(db2) // different index should produce different instances
  expect(await db0.getTables()).toEqual([])
})
