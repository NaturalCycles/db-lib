import { Readable } from 'node:stream'
import { KeyValueTuple, StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonDBCreateOptions } from '../../db.model'
import {
  CommonKeyValueDB,
  commonKeyValueDBFullSupport,
  IncrementTuple,
} from '../../kv/commonKeyValueDB'

export interface InMemoryKeyValueDBCfg {}

export class InMemoryKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: InMemoryKeyValueDBCfg = {}) {}

  support = {
    ...commonKeyValueDBFullSupport,
  }

  // data[table][id] => V
  data: StringMap<StringMap<any>> = {}

  async ping(): Promise<void> {}

  async createTable(_table: string, _opt?: CommonDBCreateOptions): Promise<void> {}

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    this.data[table] ||= {}
    ids.forEach(id => delete this.data[table]![id])
  }

  async getByIds<V>(table: string, ids: string[]): Promise<KeyValueTuple<string, V>[]> {
    this.data[table] ||= {}
    return ids.map(id => [id, this.data[table]![id]!] as KeyValueTuple<string, V>).filter(e => e[1])
  }

  async saveBatch<V>(table: string, entries: KeyValueTuple<string, V>[]): Promise<void> {
    this.data[table] ||= {}
    entries.forEach(([id, v]) => (this.data[table]![id] = v))
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    return Readable.from(Object.keys(this.data[table] || {}).slice(0, limit))
  }

  streamValues<V>(table: string, limit?: number): ReadableTyped<V> {
    return Readable.from(Object.values(this.data[table] || {}).slice(0, limit))
  }

  streamEntries<V>(table: string, limit?: number): ReadableTyped<KeyValueTuple<string, V>> {
    return Readable.from(Object.entries(this.data[table] || {}).slice(0, limit))
  }

  async count(table: string): Promise<number> {
    this.data[table] ||= {}
    return Object.keys(this.data[table]).length
  }

  async incrementBatch(table: string, entries: IncrementTuple[]): Promise<IncrementTuple[]> {
    this.data[table] ||= {}

    return entries.map(([id, by]) => {
      const newValue = ((this.data[table]![id] as number | undefined) || 0) + by
      this.data[table]![id] = newValue
      return [id, newValue]
    })
  }
}
