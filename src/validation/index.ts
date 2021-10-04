import {
  anySchema,
  arraySchema,
  booleanSchema,
  integerSchema,
  objectSchema,
  stringSchema,
} from '@naturalcycles/nodejs-lib'
import { CommonDBOptions, CommonDBSaveOptions } from '../db.model'
import { DBQuery, DBQueryFilter, dbQueryFilterOperatorValues, DBQueryOrder } from '../query/dbQuery'

export const commonDBOptionsSchema = objectSchema<CommonDBOptions>({
  onlyCache: booleanSchema.optional(),
  skipCache: booleanSchema.optional(),
})

export const commonDBSaveOptionsSchema = objectSchema<CommonDBSaveOptions>({
  excludeFromIndexes: arraySchema(stringSchema).optional(),
}).concat(commonDBOptionsSchema)

export const dbQueryFilterOperatorSchema = stringSchema.valid(...dbQueryFilterOperatorValues)

export const dbQueryFilterSchema = objectSchema<DBQueryFilter>({
  name: stringSchema,
  op: dbQueryFilterOperatorSchema,
  val: anySchema,
})

export const dbQueryOrderSchema = objectSchema<DBQueryOrder>({
  name: stringSchema,
  descending: booleanSchema.optional(),
})

export const dbQuerySchema = objectSchema<DBQuery<any>>({
  table: stringSchema,
  _filters: arraySchema(dbQueryFilterSchema).optional(),
  _limitValue: integerSchema.min(0).optional(),
  _offsetValue: integerSchema.min(0).optional(),
  _orders: arraySchema(dbQueryOrderSchema).optional(),
  _startCursor: stringSchema.optional(),
  _endCursor: stringSchema.optional(),
  _selectedFieldNames: arraySchema(stringSchema).optional(),
})
