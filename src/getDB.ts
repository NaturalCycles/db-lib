import { _memoFn } from '@naturalcycles/js-lib'
import { secretOptional } from '@naturalcycles/nodejs-lib'
import { white, yellow } from '@naturalcycles/nodejs-lib/dist/colors'
import { CommonDB } from './common.db'
import { CommonDBAdapter } from './db.model'

/**
 * Returns pre-configured DB
 *
 * @param index defaults to 1
 *
 * Requires process.env.DB${index} to be set to the name of the adapter, e.g `mysql-lib` or `db-lib/adapter/inmemory`.
 * Requires (by most adapters) process.env.SECRET_DB${index} to contain a "connection string" to that DB. Usually a JSON.stringified object,
 * but depends on the adapter.
 */
export function getDB(index = 1): CommonDB {
  return _getDB(index)
}

// Extra function to provide index=1 as default (since memo doesn't work well with default arguments)
const _getDB = _memoFn((index: number) => {
  const libName = process.env[`DB${index}`]

  if (!libName) {
    throw new Error(
      `getDB(${yellow(index)}), but process.env.${white('DB' + index)} is not defined!`,
    )
  }

  const lib: CommonDBAdapter = require(libName)

  if (!lib.getDBAdapter) {
    throw new Error(
      `DB${index}=${libName}, but require('${libName}').getDBAdapter() is not defined`,
    )
  }

  const cfg = secretOptional(`SECRET_DB${index}`)

  return lib.getDBAdapter(cfg)
})

declare global {
  namespace NodeJS {
    interface ProcessEnv {
      DB1?: string
      DB2?: string
    }
  }
}
