import { _range } from '@naturalcycles/js-lib'
import { booleanSchema, numberSchema, objectSchema, stringSchema } from '@naturalcycles/nodejs-lib'
import { BaseDBEntity, Unsaved, unsavedDBEntitySchema } from '../db.model'

const MOCK_TS_2018_06_21 = 1529539200

export const TEST_TABLE = 'TEST_TABLE'

export interface TestItem extends BaseDBEntity {
  k1: string
  k2?: string
  k3?: number
  even?: boolean
}

export const testItemSchema = objectSchema<Unsaved<TestItem>>({
  k1: stringSchema,
  k2: stringSchema.optional(),
  k3: numberSchema.optional(),
  even: booleanSchema.optional(),
}).concat(unsavedDBEntitySchema)

export function createTestItem(num = 1): TestItem {
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

export function createTestItems(count = 1): TestItem[] {
  return _range(1, count + 1).map(num => createTestItem(num))
}
