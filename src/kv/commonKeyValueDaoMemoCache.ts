import { AsyncMemoCache, MISS } from '@naturalcycles/js-lib'
import { CommonKeyValueDao } from './commonKeyValueDao'

/**
 * AsyncMemoCache implementation, backed by CommonKeyValueDao.
 *
 * Does NOT support persisting Errors, skips them instead.
 *
 * Also, does not support .clear(), as it's more dangerous than useful to actually
 * clear the whole table/cache.
 */
export class CommonKeyValueDaoMemoCache<VALUE = any> implements AsyncMemoCache<string, VALUE> {
  constructor(private dao: CommonKeyValueDao<VALUE>) {}

  async get(k: string): Promise<VALUE | typeof MISS> {
    return (await this.dao.getById(k)) || MISS
  }

  async set(k: string, v: VALUE): Promise<void> {
    await this.dao.save(k, v)
  }

  async clear(): Promise<void> {
    throw new Error(
      'CommonKeyValueDaoMemoCache.clear is not supported, because cache is expected to be persistent',
    )
  }
}
