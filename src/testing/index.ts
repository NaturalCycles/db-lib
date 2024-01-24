import { runCommonDaoTest } from './daoTest'
import { CommonDBImplementationQuirks, runCommonDBTest } from './dbTest'
import { runCommonKeyValueDBTest } from './keyValueDBTest'
import {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  TestItemBM,
  testItemBMJsonSchema,
  testItemBMSchema,
  TestItemDBM,
  TestItemTM,
  testItemTMSchema,
  TEST_TABLE,
} from './test.model'

export type { TestItemDBM, TestItemBM, TestItemTM, CommonDBImplementationQuirks }

export {
  TEST_TABLE,
  createTestItemDBM,
  createTestItemBM,
  createTestItemsDBM,
  createTestItemsBM,
  testItemBMSchema,
  testItemTMSchema,
  testItemBMJsonSchema,
  runCommonDBTest,
  runCommonDaoTest,
  runCommonKeyValueDBTest,
}
