import { CommonDB } from '../../common.db'
import { InMemoryDB } from './inMemory.db'

export function getDBAdapter(): CommonDB {
  return new InMemoryDB()
}
