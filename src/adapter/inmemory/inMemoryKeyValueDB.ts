import { StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { Readable } from 'stream'
import { CommonDBCreateOptions } from '../../db.model'
import { CommonKeyValueDB, KeyValueDBTuple } from '../../kv/commonKeyValueDB'

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

  async getByIds(table: string, ids: string[]): Promise<KeyValueDBTuple[]> {
    this.data[table] ||= {}
    return ids.map(id => [id, this.data[table]![id]!] as KeyValueDBTuple).filter(e => e[1])
  }

  async saveBatch(table: string, entries: KeyValueDBTuple[]): Promise<void> {
    this.data[table] ||= {}
    entries.forEach(([id, buf]) => (this.data[table]![id] = buf))
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    return Readable.from(Object.keys(this.data[table] || {}).slice(0, limit))
  }

  streamValues(table: string, limit?: number): ReadableTyped<Buffer> {
    return Readable.from(Object.values(this.data[table] || {}).slice(0, limit))
  }

  streamEntries(table: string, limit?: number): ReadableTyped<KeyValueDBTuple> {
    return Readable.from(Object.entries(this.data[table] || {}).slice(0, limit))
  }
}
