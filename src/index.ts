import { CacheDB, CacheDBCfg } from './adapter/cache.db'
import { InMemoryDB, queryInMemory } from './adapter/inMemory.db'
import { NoOpDB } from './adapter/noop.db'
import { SimpleFileDB, SimpleFileDBCfg } from './adapter/simpleFile.db'
import { CommonDao, CommonDaoCfg, CommonDaoLogLevel } from './common.dao'
import { CommonDB } from './common.db'
import {
  BaseDBEntity,
  baseDBEntitySchema,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  DBModelType,
  DBRelation,
  ObjectWithId,
  RunQueryResult,
  Saved,
  SavedDBEntity,
  savedDBEntitySchema,
  Unsaved,
} from './db.model'
import {
  DBQuery,
  DBQueryFilter,
  DBQueryFilterOperator,
  DBQueryOrder,
  RunnableDBQuery,
} from './dbQuery'
import {
  createdUpdatedFields,
  createdUpdatedIdFields,
  deserializeJsonField,
  idField,
  serializeJsonField,
} from './model.util'
import { dbPipelineBackup, DBPipelineBackupOptions } from './pipeline/dbPipelineBackup'
import { dbPipelineCopy, DBPipelineCopyOptions } from './pipeline/dbPipelineCopy'
import { dbPipelineRestore, DBPipelineRestoreOptions } from './pipeline/dbPipelineRestore'
import { CommonSchema, CommonSchemaField, DATA_TYPE } from './schema/common.schema'
import { CommonSchemaGenerator, CommonSchemaGeneratorCfg } from './schema/commonSchemaGenerator'
import { runCommonDaoTest } from './testing/daoTest'
import { CommonDBTestOptions, runCommonDBTest } from './testing/dbTest'
import {
  createTestItemBM,
  createTestItemDBM,
  createTestItemsBM,
  createTestItemsDBM,
  TEST_TABLE,
  TestItemBM,
  testItemBMSchema,
  TestItemDBM,
  testItemDBMSchema,
  TestItemTM,
  testItemTMSchema,
} from './testing/test.model'

export {
  DBQuery,
  DBQueryFilterOperator,
  DBQueryFilter,
  DBQueryOrder,
  RunnableDBQuery,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonDB,
  RunQueryResult,
  DBRelation,
  DBModelType,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  ObjectWithId,
  BaseDBEntity,
  SavedDBEntity,
  baseDBEntitySchema,
  savedDBEntitySchema,
  Saved,
  Unsaved,
  CommonDaoCfg,
  CommonDao,
  createdUpdatedFields,
  createdUpdatedIdFields,
  idField,
  InMemoryDB,
  queryInMemory,
  serializeJsonField,
  deserializeJsonField,
  CacheDBCfg,
  CacheDB,
  NoOpDB,
  SimpleFileDB,
  SimpleFileDBCfg,
  // Testing
  TEST_TABLE,
  createTestItemDBM,
  createTestItemBM,
  createTestItemsDBM,
  createTestItemsBM,
  TestItemDBM,
  TestItemBM,
  TestItemTM,
  testItemDBMSchema,
  testItemBMSchema,
  testItemTMSchema,
  runCommonDBTest,
  runCommonDaoTest,
  CommonDBTestOptions,
  dbPipelineBackup,
  DBPipelineBackupOptions,
  dbPipelineRestore,
  DBPipelineRestoreOptions,
  dbPipelineCopy,
  DBPipelineCopyOptions,
  CommonSchema,
  CommonSchemaField,
  DATA_TYPE,
  CommonSchemaGeneratorCfg,
  CommonSchemaGenerator,
}
