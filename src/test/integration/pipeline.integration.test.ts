import { createTestItemsDBM, DBQuery, SimpleFileDB, TEST_TABLE } from '../../index'
import { dbPipeline } from '../../pipeline.util'
import { tmpDir } from '../paths.cnst'

test('dbPipeline', async () => {
  const fileDB1 = new SimpleFileDB({
    storageDir: `${tmpDir}/storage1`,
    ndjson: true,
  })

  const fileDB2 = new SimpleFileDB({
    storageDir: `${tmpDir}/storage2`,
    ndjson: true,
  })

  const items = createTestItemsDBM(30)

  await fileDB1.saveBatch(TEST_TABLE, items)

  await dbPipeline({
    input: fileDB1.streamQuery(new DBQuery(TEST_TABLE)),
    table: TEST_TABLE,
    output: fileDB2,
  })
})
