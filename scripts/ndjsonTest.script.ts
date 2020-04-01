/*

DEBUG=nc* yarn tsn ./scripts/ndjsonTest.script.ts

 */

import { runScript } from '@naturalcycles/nodejs-lib/dist/script'
import { dbPipelineBackup, dbPipelineCopy, dbPipelineRestore } from '../src'
import { SimpleFileDB } from '../src/adapter/simplefile'
import { tmpDir } from '../src/test/paths.cnst'
import { createTestItemsDBM, TEST_TABLE } from '../src/testing'

runScript(async () => {
  const fileDB1 = new SimpleFileDB({
    storageDir: `${tmpDir}/storage1`,
    ndjson: true,
  })

  const fileDB2 = new SimpleFileDB({
    storageDir: `${tmpDir}/storage2`,
    ndjson: true,
  })

  const fileDB3 = new SimpleFileDB({
    storageDir: `${tmpDir}/storage3`,
    ndjson: true,
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
    emitSchemaFromData: true,
  })

  await dbPipelineRestore({
    db: fileDB3,
    inputDirPath: backupDir,
    recreateTables: true,
  })
})
