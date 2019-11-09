import { loadSecretsFromEnv } from '@naturalcycles/nodejs-lib'
import { NoOpDB } from '../..'
import { getDB } from '../../getDB'

test('getDB())', async () => {
  loadSecretsFromEnv()
  process.env.DB1 = `${process.cwd()}/src/adapter/noop`
  const db = getDB()
  expect(db).toBeInstanceOf(NoOpDB)
  expect(await db.getTables()).toEqual([])
})
