import { Readable } from 'node:stream'
import { StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDBCreateOptions } from '../../db.model'
import {
  CommonKeyValueDB,
  commonKeyValueDBFullSupport,
  KeyValueDBTuple,
} from '../../kv/commonKeyValueDB'

export interface InMemoryKeyValueDBCfg {}

export class InMemoryKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: InMemoryKeyValueDBCfg = {}) {}

  support = {
    ...commonKeyValueDBFullSupport,
  }

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

  async count(table: string): Promise<number> {
    this.data[table] ||= {}
    return Object.keys(this.data[table]).length
  }

  async increment(table: string, id: string, by = 1): Promise<number> {
    this.data[table] ||= {}

    const currentValue = this.data[table][id] ? parseInt(this.data[table][id].toString()) : 0
    const newValue = currentValue + by
    this.data[table][id] = Buffer.from(String(newValue))

    return newValue
  }
}
