import { deepFreeze, mockTime } from '@naturalcycles/test-lib'
import { map, toArray } from 'rxjs/operators'
import { BaseDBEntity } from './db.model'
import { DBQuery } from './dbQuery'
import { InMemoryDB } from './inMemory.db'
import { createdUpdatedFields, createdUpdatedIdFields } from './model.util'

interface TestKind extends BaseDBEntity {
  id: string
  a: string
  b: string
}

const ID = 'randomDatastoreService1'
const KIND = 'TestKind'
const db = new InMemoryDB()

beforeEach(async () => {
  // mocks
  jest.restoreAllMocks()
  mockTime()
  jest.spyOn(require('@naturalcycles/nodejs-lib'), 'stringId').mockImplementation(() => ID)
  await db.resetCache()
})

test('should throw on empty id', async () => {
  await expect(db.saveBatch(KIND, [{ a: 'b' }] as any)).rejects.toThrow()
})

test('save, load', async () => {
  const obj = {
    a: 'aa',
    b: 'bb',
    c: 1,
    ...createdUpdatedIdFields(),
  }
  deepFreeze(obj)

  await db.saveBatch(KIND, [obj])

  const [loaded] = await db.getByIds(KIND, [obj.id])
  expect(loaded).toMatchObject(obj)
  expect(loaded).toMatchSnapshot()
})

test('save with id, load, delete', async () => {
  const obj = {
    a: 'aa',
    b: 'bb',
    c: 1,
    id: 'randomid123',
    ...createdUpdatedFields(),
  }
  deepFreeze(obj)

  await db.saveBatch(KIND, [obj])

  const [loaded] = await db.getByIds(KIND, [obj.id])
  expect(loaded).toMatchSnapshot()

  const deleted = await db.deleteByIds(KIND, [obj.id])
  expect(deleted).toBe(1)
  const [loaded2] = await db.getByIds(KIND, [obj.id])
  expect(loaded2).toBeUndefined()
})

test('saveBatch, runQueryCount, deleteBy', async () => {
  const obj1: any = {
    id: 'id1',
    a: 'aa',
    b: 'b1',
    ...createdUpdatedFields(),
  }
  const obj2: any = {
    id: 'id2',
    a: 'aa',
    b: 'b2',
    ...createdUpdatedFields(),
  }
  const obj3: any = {
    id: 'id3',
    a: 'aa2',
    b: 'b3',
    ...createdUpdatedFields(),
  }
  deepFreeze(obj1)
  deepFreeze(obj2)
  deepFreeze(obj3)

  await db.saveBatch(KIND, [obj1, obj2, obj3])

  const q = new DBQuery(KIND).filterEq('a', 'aa')
  const count = await db.runQueryCount(q)
  // console.log(count)
  expect(count).toBe(2)

  expect(await db.getByIds(KIND, ['id3'])).toEqual([obj3])
  await db.deleteByQuery(new DBQuery<TestKind>(KIND).filter('b', '=', 'b3'))
  expect(await db.getByIds(KIND, ['id3'])).toEqual([])
})

test('select', async () => {
  const obj1: any = {
    id: 'id1',
    a: 'aa',
    b: 'b1',
    ...createdUpdatedFields(),
  }

  await db.saveBatch(KIND, [obj1])

  const q = new DBQuery<TestKind>(KIND).select(['a'])
  const { records } = await db.runQuery(q)
  // console.log({rows})
  expect(records).toEqual([{ a: 'aa' }])
})

test('sort', async () => {
  const obj1: any = {
    id: 'id1',
    a: 'aa',
    b: 'b1',
    ...createdUpdatedFields(),
  }
  const obj2: any = {
    id: 'id2',
    a: 'aa2',
    b: 'b12',
    ...createdUpdatedFields(),
  }

  await db.saveBatch(KIND, [obj1, obj2])

  const q = new DBQuery<TestKind>(KIND).order('a')
  const { records } = await db.runQuery(q)
  const [r1, r2] = records
  expect([r1.id, r2.id]).toEqual(['id1', 'id2'])

  const q2 = new DBQuery<TestKind>(KIND).order('a', true)
  const { records: records2 } = await db.runQuery(q2)
  const [r11, r12] = records2
  expect([r11.id, r12.id]).toEqual(['id2', 'id1'])
})

function mockTestKindItems(): TestKind[] {
  return [
    {
      id: 'id1',
      a: 'aa',
      b: 'b1',
      ...createdUpdatedFields(),
    },
    {
      id: 'id2',
      a: 'aa',
      b: 'b2',
      ...createdUpdatedFields(),
    },
    {
      id: 'id3',
      a: 'aa2',
      b: 'b3',
      ...createdUpdatedFields(),
    },
  ]
}

test('streamQuery', async () => {
  const items = mockTestKindItems()
  await db.saveBatch(KIND, items)

  const q = new DBQuery<TestKind>(KIND).filterEq('a', 'aa')
  const rows = await db
    .streamQuery(q)
    .pipe(
      // debugging
      // mergeMap(async row => {
      //   console.log({row})
      //   await new Promise(r => setTimeout(r, 500))
      //   return row
      // }, 1),
      // reduce((rows, row) => rows.concat(row), [] as TestKind[]),
      toArray(),
    )
    .toPromise()
  // console.log('done', rows)

  expect(rows.length).toBe(2)
  expect(rows).toMatchSnapshot()

  const ids = await db
    .streamQuery(q.select(['id']))
    .pipe(
      map(row => row.id),
      toArray(),
    )
    .toPromise()
  // console.log(ids)
  expect(ids).toEqual(['id1', 'id2'])
})
