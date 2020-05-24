import {
  transformJsonParse,
  transformSplit,
  transformToNDJson,
  writablePushToArray,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import * as fs from 'fs-extra'
import { Readable } from 'stream'
import { createGzip, createUnzip } from 'zlib'
import { SavedDBEntity } from '../../db.model'
import { DBTransaction } from '../../dbTransaction'
import { FileDB } from './file.db'
import { FileDBPersistencePlugin } from './file.db.model'

export interface LocalFilePersistencePluginCfg {
  /**
   * @default ./tmp/localdb
   */
  storagePath: string

  /**
   * @default true
   */
  gzip: boolean
}

/**
 * Persists in local filesystem as ndjson.
 */
export class LocalFilePersistencePlugin implements FileDBPersistencePlugin {
  constructor(cfg: Partial<LocalFilePersistencePluginCfg> = {}) {
    this.cfg = {
      storagePath: './tmp/localdb',
      gzip: true,
      ...cfg,
    }
  }

  public cfg!: LocalFilePersistencePluginCfg

  async ping(): Promise<void> {}

  async getTables(): Promise<string[]> {
    return (await fs.readdir(this.cfg.storagePath))
      .filter(f => f.includes('.ndjson'))
      .map(f => f.split('.ndjson')[0])
  }

  async loadFile<DBM extends SavedDBEntity>(table: string): Promise<DBM[]> {
    await fs.ensureDir(this.cfg.storagePath)
    const ext = `ndjson${this.cfg.gzip ? '.gz' : ''}`
    const filePath = `${this.cfg.storagePath}/${table}.${ext}`

    if (!(await fs.pathExists(filePath))) return []

    const transformUnzip = this.cfg.gzip ? [createUnzip()] : []

    const rows: DBM[] = []

    await _pipeline([
      fs.createReadStream(filePath),
      ...transformUnzip,
      transformSplit(), // splits by \n
      transformJsonParse(),
      writablePushToArray(rows),
    ])

    return rows
  }

  async saveFile<DBM extends SavedDBEntity>(table: string, dbms: DBM[]): Promise<void> {
    await fs.ensureDir(this.cfg.storagePath)
    const ext = `ndjson${this.cfg.gzip ? '.gz' : ''}`
    const filePath = `${this.cfg.storagePath}/${table}.${ext}`
    const transformZip = this.cfg.gzip ? [createGzip()] : []

    await _pipeline([
      Readable.from(dbms),
      transformToNDJson(),
      ...transformZip,
      fs.createWriteStream(filePath),
    ])
  }

  transaction(db: FileDB): DBTransaction {
    return new DBTransaction(db)
  }
}
