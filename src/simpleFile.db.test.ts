import { SimpleFileDB } from './simpleFile.db'
import { tmpDir } from './test/paths.cnst'
import { runCommonDaoTest } from './testing/daoTest'
import { runCommonDBTest } from './testing/dbTest'

const db = new SimpleFileDB({
  storageDir: `${tmpDir}/storage`,
})

test('runCommonDBTest', async () => {
  await runCommonDBTest(db)
})

test('runCommonDaoTest', async () => {
  await runCommonDaoTest(db)
})
