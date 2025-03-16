import { test } from 'vitest'
import { InMemoryDB } from '../adapter/inmemory/inMemory.db'
import { tmpDir } from '../test/paths.cnst'
import { createTestItemsDBM, TEST_TABLE } from '../testing'
import { dbPipelineBackup } from './dbPipelineBackup'

test('dbPipelineSaveToNDJson', async () => {
  const db = new InMemoryDB()

  const items = createTestItemsDBM(70)
  await db.saveBatch(TEST_TABLE, items)

  await dbPipelineBackup({
    db,
    outputDirPath: tmpDir,
    gzip: false,
  })
})
