import { _range } from '@naturalcycles/js-lib'
import { booleanSchema, objectSchema, stringSchema } from '@naturalcycles/nodejs-lib'
import { BaseDBEntity, Unsaved, unsavedDBEntitySchema } from '../db.model'

export const TEST_KIND = 'TestKind'

export interface TestKindBM extends BaseDBEntity {
  a: string
  b?: string
  c?: boolean
}

export const testKindUnsavedBMSchema = objectSchema<Unsaved<TestKindBM>>({
  a: stringSchema,
  b: stringSchema.optional(),
  c: booleanSchema.optional(),
}).concat(unsavedDBEntitySchema)

export function mockTestData(): Unsaved<TestKindBM>[] {
  return _range(1, 5).map(num => ({
    id: `id${num}`,
    a: `a${num}`,
    b: `b${num}`,
    c: num % 2 === 0,
  }))
}
