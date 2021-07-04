import { StringMap, _stringMapEntries } from '@naturalcycles/js-lib'
import { CommonDBCreateOptions } from '../../db.model'
import { CommonKeyValueDB } from '../../kv/commonKeyValueDB'

export interface InMemoryKeyValueDBCfg {}

export class InMemoryKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: InMemoryKeyValueDBCfg = {}) {}

  // data[table][id] > Buffer
  data: StringMap<StringMap<Buffer>> = {}

  async ping(): Promise<void> {}

  async createTable(_table: string, _opt?: CommonDBCreateOptions): Promise<void> {}

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    this.data[table] ||= {}
    ids.forEach(id => delete this.data[table]![id])
  }

  async getByIds(table: string, ids: string[]): Promise<Buffer[]> {
    this.data[table] ||= {}
    return ids.map(id => this.data[table]![id]!).filter(Boolean)
  }

  async saveBatch(table: string, batch: StringMap<Buffer>): Promise<void> {
    this.data[table] ||= {}
    _stringMapEntries(batch).forEach(([id, b]) => (this.data[table]![id] = b))
  }
}
