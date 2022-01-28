import { AsyncMemoCache } from '@naturalcycles/js-lib'
import { CommonKeyValueDao } from './commonKeyValueDao'

export class CommonKeyValueDaoMemoCache<VALUE = any> implements AsyncMemoCache<string, VALUE> {
  constructor(private dao: CommonKeyValueDao<VALUE>) {}

  async get(k: string): Promise<VALUE | undefined> {
    return (await this.dao.getById(k)) || undefined
  }

  async set(k: string, v: VALUE): Promise<void> {
    await this.dao.save(k, v)
  }

  async clear(): Promise<void> {
    throw new Error(
      'CommonKeyValueDaoCacheFactory.clear is not supported, because cache is expected to be persistent',
    )
  }
}
