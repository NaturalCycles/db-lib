import { CommonDB } from '../../common.db'
import { SimpleFileDB } from './simpleFile.db'

export function getDBAdapter(cfgStr: string = '{}'): CommonDB {
  const cfg = JSON.parse(cfgStr)
  return new SimpleFileDB(cfg)
}
