export interface CommonSchema<T = any> {
  /**
   * Name of the Table
   */
  table: string

  fields: CommonSchemaField[]
  // can possibly have `meta` with table meta-information
}

export interface CommonSchemaField {
  name: string
  type: DATA_TYPE

  /**
   * If it's an Array - then parentType should tell "array of what?"
   */
  arrayOf?: CommonSchemaField

  /**
   * If it's an Object - defined the CommonSchemaField of that object
   */
  objectFields?: CommonSchemaField[]

  notNull?: boolean

  /**
   * Applicable to certain fields, e.g String, to be able to autodetect limits for certain Databases
   */
  maxLen?: number
  minLen?: number
  // avgLen?: number
}

export enum DATA_TYPE {
  UNKNOWN = 'UNKNOWN',
  NULL = 'NULL',
  STRING = 'STRING',
  INT = 'INT',
  FLOAT = 'FLOAT',
  BOOLEAN = 'BOOLEAN',
  BINARY = 'BINARY',
  LOCAL_DATE = 'LOCAL_DATE', // ISO
  TIMESTAMP = 'TIMESTAMP', // unix timestamp
  // Semi-structured
  ARRAY = 'ARRAY',
  OBJECT = 'OBJECT',
}
