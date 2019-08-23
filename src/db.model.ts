import { idSchema, objectSchema, unixTimestampSchema, verSchema } from '@naturalcycles/nodejs-lib'
import { Observable } from 'rxjs'
import { Merge } from 'type-fest'
import { DBQuery } from './dbQuery'

/**
 * All properties default to undefined.
 */
export interface CommonDaoOptions extends CommonDBOptions {
  skipValidation?: boolean
  throwOnError?: boolean
  preserveUpdatedCreated?: boolean
}

/**
 * All properties default to undefined.
 */
export interface CommonDaoSaveOptions extends CommonDaoOptions, CommonDBSaveOptions {}

/**
 * All properties default to undefined.
 */
export interface CommonDBOptions {
  /**
   * @default false
   */
  skipCache?: boolean

  /**
   * @default false
   */
  onlyCache?: boolean
}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions extends CommonDBOptions {
  excludeFromIndexes?: string[]
}

export interface CommonDB {
  /**
   * If table not specified - reset all DB.
   */
  resetCache (table?: string): Promise<void>

  // GET
  getByIds<DBM extends BaseDBEntity> (
    table: string,
    ids: string[],
    opts?: CommonDBOptions,
  ): Promise<DBM[]>

  // QUERY
  runQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]>
  runQueryCount<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number>
  streamQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM>

  // SAVE
  saveBatch<DBM extends BaseDBEntity> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<void>

  // DELETE
  /**
   * @returns number of deleted items
   */
  deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<number>
  deleteByQuery<DBM extends BaseDBEntity> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number>
}

export enum DBRelation {
  ONE_TO_ONE = 'ONE_TO_ONE',
  ONE_TO_MANY = 'ONE_TO_MANY',
}

export enum DBModelType {
  DBM = 'DBM',
  BM = 'BM',
}

export interface CreatedUpdated {
  created: number
  updated: number
}

export interface CreatedUpdatedId extends CreatedUpdated {
  id: string
}

export interface CreatedUpdatedVer {
  created: number
  updated: number
  _ver?: number
}

export interface ObjectWithId {
  id: string
}

export interface BaseDBEntity {
  id: string
  created: number
  updated: number
  _ver?: number
}

export interface UnsavedDBEntity {
  id?: string
  created?: number
  updated?: number
  _ver?: number
}

export type Unsaved<E> = Merge<E, UnsavedDBEntity>

export const baseDBEntitySchema = objectSchema<BaseDBEntity>({
  id: idSchema,
  created: unixTimestampSchema,
  updated: unixTimestampSchema,
  _ver: verSchema.optional(),
})

export const unsavedDBEntitySchema = objectSchema<UnsavedDBEntity>({
  id: idSchema.optional(),
  created: unixTimestampSchema.optional(),
  updated: unixTimestampSchema.optional(),
  _ver: verSchema.optional(),
})
