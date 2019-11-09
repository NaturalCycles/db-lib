import { CommonDB } from '../../common.db'
import { NoOpDB } from './noop.db'

export function getDBAdapter(): CommonDB {
  return new NoOpDB()
}
