import { CommonDB } from '../../common.db'
import { InMemoryDB } from './inMemory.db'

export function getDBAdapter(): CommonDB {
  return new InMemoryDB()
}

export // InMemoryDB, // no, otherwise it's double-exported, which can confuse IDEs
 {}
