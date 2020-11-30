import { runCommonDaoTest } from './daoTest'
import {
  CommonDBImplementationFeatures,
  CommonDBImplementationQuirks,
  runCommonDBTest,
} from './dbTest'
import {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  getTestItemSchema,
  TestItemBM,
  testItemBMSchema,
  TestItemDBM,
  testItemDBMSchema,
  TestItemTM,
  testItemTMSchema,
  TEST_TABLE,
} from './test.model'

export type {
  TestItemDBM,
  TestItemBM,
  TestItemTM,
  CommonDBImplementationFeatures,
  CommonDBImplementationQuirks,
}

export {
  TEST_TABLE,
  createTestItemDBM,
  createTestItemBM,
  createTestItemsDBM,
  createTestItemsBM,
  testItemDBMSchema,
  testItemBMSchema,
  testItemTMSchema,
  getTestItemSchema,
  runCommonDBTest,
  runCommonDaoTest,
}
