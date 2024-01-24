/*

yarn tsn cannon

 */

/* eslint-disable unused-imports/no-unused-vars */
import { expressFunctionFactory, runCannon } from '@naturalcycles/bench-lib'
import { _omit } from '@naturalcycles/js-lib'
import { getValidationResult, stringId, runScript } from '@naturalcycles/nodejs-lib'
import { CommonDao, InMemoryDB } from '../src'
import { createTestItemsBM, testItemBMSchema, TEST_TABLE } from '../src/testing'

runScript(async () => {
  await runCannon(
    {
      // register1: expressFunctionFactory(register1),
      // register2: expressFunctionFactory(register2),
      register3: expressFunctionFactory(register3),
      // register4: expressFunctionFactory(register4),
      registerFull: expressFunctionFactory(registerFull),
      validate1: expressFunctionFactory(validate1),
    },
    {
      runs: 1,
      duration: 5,
      cooldown: 1,
      renderLatencyTable: false,
      name: 'cannon2',
    },
  )
})

const db = new InMemoryDB()
const dao = new CommonDao({
  table: TEST_TABLE,
  db,
  bmSchema: testItemBMSchema,
})

async function register1(): Promise<any> {
  const item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item.id = stringId()
  return { item }
}

async function register2(): Promise<any> {
  const item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item.id = stringId()
  await db.saveBatch(TEST_TABLE, [item])
  return { item }
}

async function register3(): Promise<any> {
  let item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item = await dao.save(item, { skipConversion: true })
  return { item }
}

async function register4(): Promise<any> {
  let item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item = await dao.save(item, { skipValidation: true })
  return { item }
}

async function registerFull(): Promise<any> {
  let item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item = await dao.save(item)
  return { item }
}

async function validate1(): Promise<any> {
  const item = createTestItemsBM(1).map(r => _omit(r, ['id']))[0]!
  item.id = stringId()
  getValidationResult(item, testItemBMSchema)

  return { item }
}
