import { InMemoryDB, InMemoryDBCfg } from './adapter/inmemory/inMemory.db'
import { InMemoryKeyValueDB, InMemoryKeyValueDBCfg } from './adapter/inmemory/inMemoryKeyValueDB'
import { queryInMemory } from './adapter/inmemory/queryInMemory'
import { BaseCommonDB } from './base.common.db'
import { DBLibError } from './cnst'
import { CommonDB } from './common.db'
import { CommonDao } from './commondao/common.dao'
import {
  CommonDaoAnonymizeHook,
  CommonDaoBeforeBMToDBMHook,
  CommonDaoBeforeBMToTMHook,
  CommonDaoBeforeCreateHook,
  CommonDaoBeforeDBMToBMHook,
  CommonDaoBeforeDBMValidateHook,
  CommonDaoBeforeTMToBMHook,
  CommonDaoCfg,
  CommonDaoCreateIdHook,
  CommonDaoCreateOptions,
  CommonDaoLogLevel,
  CommonDaoOptions,
  CommonDaoParseNaturalIdHook,
  CommonDaoSaveOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
} from './commondao/common.dao.model'
import {
  BaseDBEntity,
  baseDBEntitySchema,
  CommonDBAdapter,
  CommonDBCreateOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  DBDeleteByIdsOperation,
  DBModelType,
  DBOperation,
  DBRelation,
  DBSaveBatchOperation,
  ObjectWithId,
  RunQueryResult,
  Saved,
  SavedDBEntity,
  savedDBEntitySchema,
  Unsaved,
} from './db.model'
import { getDB } from './getDB'
import { CommonKeyValueDao, CommonKeyValueDaoCfg } from './kv/commonKeyValueDao'
import { CommonKeyValueDB, KeyValueDBTuple } from './kv/commonKeyValueDB'
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
import {
  DBQuery,
  DBQueryFilter,
  DBQueryFilterOperator,
  DBQueryFilterOperatorValues,
  DBQueryOrder,
  RunnableDBQuery,
} from './query/dbQuery'
import { CommonSchema, CommonSchemaField, DATA_TYPE } from './schema/common.schema'
import { CommonSchemaGenerator, CommonSchemaGeneratorCfg } from './schema/commonSchemaGenerator'
import { DBTransaction, RunnableDBTransaction } from './transaction/dbTransaction'
import { commitDBTransactionSimple, mergeDBOperations } from './transaction/dbTransaction.util'

export type {
  DBQueryFilterOperator,
  DBQueryFilter,
  DBQueryOrder,
  CommonDaoCreateOptions,
  CommonDaoOptions,
  CommonDaoSaveOptions,
  CommonDaoStreamForEachOptions,
  CommonDaoStreamOptions,
  CommonDBOptions,
  CommonDBSaveOptions,
  CommonDBStreamOptions,
  CommonDBCreateOptions,
  CommonDB,
  RunQueryResult,
  CreatedUpdated,
  CreatedUpdatedId,
  CreatedUpdatedVer,
  ObjectWithId,
  BaseDBEntity,
  SavedDBEntity,
  Saved,
  Unsaved,
  CommonDaoCfg,
  CommonDaoCreateIdHook,
  CommonDaoParseNaturalIdHook,
  CommonDaoBeforeCreateHook,
  CommonDaoBeforeDBMValidateHook,
  CommonDaoBeforeDBMToBMHook,
  CommonDaoBeforeBMToDBMHook,
  CommonDaoBeforeTMToBMHook,
  CommonDaoBeforeBMToTMHook,
  CommonDaoAnonymizeHook,
  InMemoryDBCfg,
  InMemoryKeyValueDBCfg,
  DBPipelineBackupOptions,
  DBPipelineRestoreOptions,
  DBPipelineCopyOptions,
  CommonSchema,
  CommonSchemaField,
  CommonSchemaGeneratorCfg,
  CommonDBAdapter,
  DBOperation,
  DBSaveBatchOperation,
  DBDeleteByIdsOperation,
  CommonKeyValueDB,
  CommonKeyValueDaoCfg,
  KeyValueDBTuple,
}

export {
  DBQuery,
  DBQueryFilterOperatorValues,
  RunnableDBQuery,
  CommonDaoLogLevel,
  DBRelation,
  DBModelType,
  baseDBEntitySchema,
  savedDBEntitySchema,
  CommonDao,
  createdUpdatedFields,
  createdUpdatedIdFields,
  idField,
  InMemoryDB,
  InMemoryKeyValueDB,
  queryInMemory,
  serializeJsonField,
  deserializeJsonField,
  dbPipelineBackup,
  dbPipelineRestore,
  dbPipelineCopy,
  DATA_TYPE,
  getDB,
  DBLibError,
  BaseCommonDB,
  DBTransaction,
  RunnableDBTransaction,
  mergeDBOperations,
  commitDBTransactionSimple,
  CommonSchemaGenerator,
  CommonKeyValueDao,
}
