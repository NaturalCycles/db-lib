import { runCommonDaoTest } from './daoTest'
import { CommonDBImplementationQuirks, runCommonDBTest } from './dbTest'
import { runCommonKeyValueDBTest } from './keyValueDBTest'
import {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  TEST_TABLE,
  TestItemBM,
  testItemBMJsonSchema,
  testItemBMSchema,
  TestItemDBM,
  TestItemTM,
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
  runCommonKeyValueDBTest,
  TEST_TABLE,
  testItemBMJsonSchema,
  testItemBMSchema,
  testItemTMSchema,
}
