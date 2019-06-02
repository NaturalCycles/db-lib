import { arrayRange } from '@naturalcycles/js-lib'
import { booleanSchema, objectSchema, stringSchema } from '@naturalcycles/nodejs-lib'
import { BaseDBEntity, baseDBEntitySchema } from '../db.model'
import { createdUpdatedFields } from '../model.util'

export const TEST_KIND = 'TestKind'

export interface TestKindBM extends BaseDBEntity {
  a: string
  b?: string
  c?: boolean
}

export const testKindBMSchema = objectSchema<TestKindBM>({
  a: stringSchema,
  b: stringSchema.optional(),
  c: booleanSchema.optional(),
}).concat(baseDBEntitySchema)

export function mockTestData (): TestKindBM[] {
  return arrayRange(1, 5).map(num => ({
    id: `id${num}`,
    a: `a${num}`,
    b: `b${num}`,
    c: num % 2 === 0,
    ...createdUpdatedFields(),
  }))
}
