import { runCommonDaoTest } from './commonDaoTest'
import type { CommonDBImplementationQuirks } from './commonDBTest'
import { runCommonDBTest } from './commonDBTest'
import { runCommonKeyValueDaoTest } from './keyValueDaoTest'
import { runCommonKeyValueDBTest } from './keyValueDBTest'
import type { TestItemBM, TestItemDBM, TestItemTM } from './test.model'
import {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  TEST_TABLE,
  testItemBMJsonSchema,
  testItemBMSchema,
  testItemTMSchema,
} from './test.model'

export type { CommonDBImplementationQuirks, TestItemBM, TestItemDBM, TestItemTM }

export {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  runCommonDaoTest,
  runCommonDBTest,
  runCommonKeyValueDaoTest,
  runCommonKeyValueDBTest,
  TEST_TABLE,
  testItemBMJsonSchema,
  testItemBMSchema,
  testItemTMSchema,
}
