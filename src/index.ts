import { CacheDB, CacheDBCfg } from './cache.db'
import { CommonDao, CommonDaoCfg, CommonDaoLogLevel } from './common.dao'
import {
  BaseDBEntity,
  baseDBEntitySchema,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDB,
  CommonDBOptions,
  CommonDBSaveOptions,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  DBModelType,
  DBRelation,
  ObjectWithId,
  Unsaved,
  UnsavedDBEntity,
  unsavedDBEntitySchema,
} from './db.model'
import { DBQuery, DBQueryFilter, DBQueryFilterOperator, DBQueryOrder } from './dbQuery'
import { InMemoryDB } from './inMemory.db'
import {
  createdUpdatedFields,
  createdUpdatedIdFields,
  deserializeJsonField,
  idField,
  serializeJsonField,
} from './model.util'
import { NoOpDB } from './noop.db'
import { SimpleFileDB, SimpleFileDBCfg } from './simpleFile.db'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'
import {
  createTestItem,
  createTestItems,
  TEST_TABLE,
  TestItem,
  testItemSchema,
} from './testing/test.model'

export {
  DBQuery,
  DBQueryFilterOperator,
  DBQueryFilter,
  DBQueryOrder,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDB,
  DBRelation,
  DBModelType,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  ObjectWithId,
  BaseDBEntity,
  baseDBEntitySchema,
  unsavedDBEntitySchema,
  UnsavedDBEntity,
  Unsaved,
  CommonDaoCfg,
  CommonDao,
  createdUpdatedFields,
  createdUpdatedIdFields,
  idField,
  InMemoryDB,
  serializeJsonField,
  deserializeJsonField,
  CacheDBCfg,
  CacheDB,
  NoOpDB,
  SimpleFileDB,
  SimpleFileDBCfg,
  // Testing
  TEST_TABLE,
  TestItem,
  testItemSchema,
  createTestItem,
  createTestItems,
  runCommonDBTest,
  runCommonDaoTest,
}
