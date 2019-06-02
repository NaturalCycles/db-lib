import { idSchema, objectSchema, unixTimestampSchema, verSchema } from '@naturalcycles/nodejs-lib'
import { Observable } from 'rxjs'
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
export interface CommonDBOptions {}

/**
 * All properties default to undefined.
 */
export interface CommonDBSaveOptions extends CommonDBOptions {
  excludeFromIndexes?: string[]
}

export interface CommonDB {
  // GET
  getByIds<DBM = any> (table: string, ids: string[], opts?: CommonDBOptions): Promise<DBM[]>

  // QUERY
  runQuery<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<DBM[]>
  runQueryCount<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Promise<number>
  streamQuery<DBM = any> (q: DBQuery<DBM>, opts?: CommonDBOptions): Observable<DBM>

  // SAVE
  saveBatch<DBM extends BaseDBEntity = any> (
    table: string,
    dbms: DBM[],
    opts?: CommonDBSaveOptions,
  ): Promise<DBM[]>

  // DELETE
  /**
   * @returns array of deleted items' ids
   */
  deleteByIds (table: string, ids: string[], opts?: CommonDBOptions): Promise<string[]>
  deleteBy (
    table: string,
    by: string,
    value: any,
    limit?: number,
    opts?: CommonDBOptions,
  ): Promise<string[]>
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

export const baseDBEntitySchema = objectSchema<BaseDBEntity>({
  id: idSchema,
  created: unixTimestampSchema,
  updated: unixTimestampSchema,
  _ver: verSchema.optional(),
})
