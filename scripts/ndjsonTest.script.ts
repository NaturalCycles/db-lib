/*

DEBUG=nc* yarn tsn ./scripts/ndjsonTest.script.ts

 */

import { runScript } from '@naturalcycles/nodejs-lib'
import { createTestItemsDBM, dbPipelineSaveToNDJson, SimpleFileDB, TEST_TABLE } from '../src'
import { dbPipelineCopy } from '../src/pipeline/dbPipelineCopy'
import { dbPipelineLoadFromNDJson } from '../src/pipeline/dbPipelineLoadFromNDJson'
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
  })

  const backupDir = `${tmpDir}/backup`

  await dbPipelineSaveToNDJson({
    db: fileDB2,
    outputDirPath: backupDir,
  })

  await dbPipelineLoadFromNDJson({
    db: fileDB3,
    inputDirPath: backupDir,
  })
})
