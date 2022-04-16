import { runCommonDaoTest } from './daoTest'
import {
  CommonDBImplementationFeatures,
  CommonDBImplementationQuirks,
  runCommonDBTest,
} from './dbTest'
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
  testItemDBMJsonSchema,
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
  testItemBMJsonSchema,
  testItemDBMJsonSchema,
  runCommonDBTest,
  runCommonDaoTest,
  runCommonKeyValueDBTest,
}
