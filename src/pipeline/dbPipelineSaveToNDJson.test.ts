import { createTestItemsDBM, TEST_TABLE } from '..'
import { InMemoryDB } from '../inMemory.db'
import { tmpDir } from '../test/paths.cnst'
import { dbPipelineSaveToNDJson } from './dbPipelineSaveToNDJson'

test('dbPipelineSaveToNDJson', async () => {
  const db = new InMemoryDB()

  const items = createTestItemsDBM(70)
  await db.saveBatch(TEST_TABLE, items)

  await dbPipelineSaveToNDJson({
    db,
    outputDirPath: `${tmpDir}`,
    gzip: false,
  })
})
