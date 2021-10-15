import { jsonSchema, JsonSchemaObject, _range, BaseDBEntity } from '@naturalcycles/js-lib'
import {
  binarySchema,
  booleanSchema,
  numberSchema,
  objectSchema,
  stringSchema,
} from '@naturalcycles/nodejs-lib'
import { baseDBEntitySchema, Saved, savedDBEntitySchema } from '../db.model'

const MOCK_TS_2018_06_21 = 1529539200

export const TEST_TABLE = 'TEST_TABLE'

export interface TestItemBM extends BaseDBEntity {
  k1: string
  k2?: string
  k3?: number
  even?: boolean
  b1?: Buffer
}

export interface TestItemDBM extends Saved<TestItemBM> {}

export interface TestItemTM {
  k1: string
  even?: boolean
}

export const testItemBMSchema = objectSchema<TestItemBM>({
  k1: stringSchema,
  k2: stringSchema.optional(),
  k3: numberSchema.optional(),
  even: booleanSchema.optional(),
  b1: binarySchema.optional(),
}).concat(baseDBEntitySchema)

export const testItemDBMSchema = objectSchema<TestItemDBM>({
  k1: stringSchema,
  k2: stringSchema.optional(),
  k3: numberSchema.optional(),
  even: booleanSchema.optional(),
  b1: binarySchema.optional(),
}).concat(savedDBEntitySchema)

export const testItemTMSchema = objectSchema<TestItemTM>({
  k1: stringSchema,
  even: booleanSchema.optional(),
})

export const testItemBMJsonSchema = jsonSchema
  .rootObject<TestItemBM>({
    k1: jsonSchema.string(),
    k2: jsonSchema.string().optional(),
    k3: jsonSchema.number().optional(),
    even: jsonSchema.boolean().optional(),
    b1: jsonSchema.buffer().optional(),
  })
  .baseDBEntity()

export const testItemDBMJsonSchema = jsonSchema.rootObject<TestItemDBM>({
  // todo: figure out how to not copy-paste these 3 fields
  id: jsonSchema.string(),
  created: jsonSchema.unixTimestamp(),
  updated: jsonSchema.unixTimestamp(),
  k1: jsonSchema.string(),
  k2: jsonSchema.string().optional(),
  k3: jsonSchema.number().optional(),
  even: jsonSchema.boolean().optional(),
  b1: jsonSchema.buffer().optional(),
})

export function createTestItemDBM(num = 1): TestItemDBM {
  return {
    id: `id${num}`,
    k1: `v${num}`,
    k2: `v${num * 2}`,
    k3: num,
    even: num % 2 === 0,
    created: MOCK_TS_2018_06_21,
    updated: MOCK_TS_2018_06_21,
  }
}

export function createTestItemBM(num = 1): Saved<TestItemBM> {
  return createTestItemDBM(num)
}

export function createTestItemsDBM(count = 1): TestItemDBM[] {
  return _range(1, count + 1).map(num => createTestItemDBM(num))
}

export function createTestItemsBM(count = 1): Saved<TestItemBM>[] {
  return _range(1, count + 1).map(num => createTestItemBM(num))
}

const testItemJsonSchema = jsonSchema
  .object<TestItemDBM>({
    id: jsonSchema.string(),
    k1: jsonSchema.string(),
    k2: jsonSchema.string(),
    k3: jsonSchema.number(),
    even: jsonSchema.boolean(),
    created: jsonSchema.unixTimestamp(),
    updated: jsonSchema.unixTimestamp(),
  })
  .build()

export function getTestItemSchema(): JsonSchemaObject<TestItemDBM> {
  // return CommonSchemaGenerator.generateFromRows({ table: TEST_TABLE }, createTestItemsDBM())
  return testItemJsonSchema
}
