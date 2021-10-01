/*

DEBUG=nc* yarn tsn ndjsonTest.script.ts

 */

import { runScript } from '@naturalcycles/nodejs-lib/dist/script'
import { dbPipelineBackup, dbPipelineCopy, dbPipelineRestore, InMemoryDB } from '../src'
import { tmpDir } from '../src/test/paths.cnst'
import { createTestItemsDBM, TEST_TABLE } from '../src/testing'

runScript(async () => {
  const fileDB1 = new InMemoryDB({
    // persistenceEnabled: true,
    // persistentStoragePath: `${tmpDir}/storage1`,
  })

  const fileDB2 = new InMemoryDB({
    // persistenceEnabled: true,
    // persistentStoragePath: `${tmpDir}/storage2`,
  })

  const fileDB3 = new InMemoryDB({
    // persistenceEnabled: true,
    // persistentStoragePath: `${tmpDir}/storage3`,
  })

  const items = createTestItemsDBM(30)

  await fileDB1.saveBatch(TEST_TABLE, items)

  await dbPipelineCopy({
    dbInput: fileDB1,
    dbOutput: fileDB2,
    // limit: 2,
  })

  const backupDir = `${tmpDir}/backup`

  await dbPipelineBackup({
    db: fileDB2,
    outputDirPath: backupDir,
    // emitSchemaFromDB: true,
    // emitSchemaFromData: true,
  })

  await dbPipelineRestore({
    db: fileDB3,
    inputDirPath: backupDir,
    recreateTables: true,
  })
})
