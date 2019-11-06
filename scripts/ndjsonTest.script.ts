/*

DEBUG=nc* yarn tsn ./scripts/ndjsonTest.script.ts

 */

import { runScript } from '@naturalcycles/nodejs-lib'
import { createTestItemsDBM, dbPipelineBackup, SimpleFileDB, TEST_TABLE } from '../src'
import { dbPipelineCopy, dbPipelineRestore } from '../src'
import { tmpDir } from '../src/test/paths.cnst'

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
  })

  await dbPipelineRestore({
    db: fileDB3,
    inputDirPath: backupDir,
  })
})
