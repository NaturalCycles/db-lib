import { Readable } from 'node:stream'
import { _deepCopy, _omit, _pick, _sortBy, nowUnix } from '@naturalcycles/js-lib'
import { _pipeline } from '@naturalcycles/nodejs-lib'
import { CommonDaoLogLevel, DBQuery } from '..'
import { CommonDB } from '../common.db'
import { CommonDao } from '../commondao/common.dao'
import { TestItemBM } from '.'
import { CommonDBImplementationQuirks, expectMatch } from './dbTest'
import {
  createTestItemBM,
  createTestItemsBM,
  TEST_TABLE,
  testItemBMJsonSchema,
  testItemBMSchema,
} from './test.model'

export function runCommonDaoTest(db: CommonDB, quirks: CommonDBImplementationQuirks = {}): void {
  const { support } = db
  const dao = new CommonDao({
    table: TEST_TABLE,
    db,
    bmSchema: testItemBMSchema,
    logStarted: true,
    logLevel: CommonDaoLogLevel.DATA_FULL,
  })

  const items = createTestItemsBM(3)
  const itemsClone = _deepCopy(items)
  // deepFreeze(items) // mutation of id/created/updated is allowed now! (even expected)
  const item1 = items[0]!

  const expectedItems = items.map(i => ({
    ...i,
    updated: expect.any(Number),
  }))

  test('ping', async () => {
    await dao.ping()
  })

  // CREATE TABLE, DROP
  if (support.createTable) {
    test('createTable, dropIfExists=true', async () => {
      await dao.createTable(testItemBMJsonSchema, { dropIfExists: true })
    })
  }

  if (support.queries) {
    // DELETE ALL initially
    test('deleteByIds test items', async () => {
      const rows = await dao.query().select(['id']).runQuery()
      await db.deleteByQuery(
        DBQuery.create(TEST_TABLE).filter(
          'id',
          'in',
          rows.map(r => r.id),
        ),
      )
    })

    // QUERY empty
    test('runQuery(all), runQueryCount should return empty', async () => {
      expect(await dao.query().runQuery()).toEqual([])
      expect(await dao.query().runQueryCount()).toBe(0)
    })
  }

  // GET empty
  test('getByIds(item1.id) should return empty', async () => {
    const [item1Loaded] = await dao.getByIds([item1.id])
    expect(item1Loaded).toBeUndefined()
    expect(await dao.getById(item1.id)).toBeNull()
  })

  test('getByIds([]) should return []', async () => {
    expect(await dao.getByIds([])).toEqual([])
  })

  test('getByIds(...) should return empty', async () => {
    expect(await dao.getByIds(['abc', 'abcd'])).toEqual([])
  })

  // SAVE
  if (support.nullValues) {
    test('should allow to save and load null values', async () => {
      const item3 = {
        ...createTestItemBM(3),
        k2: null,
      }
      // deepFreeze(item3) // no, Dao is expected to mutate object to add id, created, updated
      await dao.save(item3)
      const item3Loaded = await dao.requireById(item3.id)
      expectMatch([item3], [item3Loaded], quirks)
      expect(item3Loaded.k2).toBeNull()
      expect(Object.keys(item3)).toContain('k2')
      expect(item3.k2).toBeNull()
    })
  }

  test('undefined values should not be saved/loaded', async () => {
    const item3 = {
      ...createTestItemBM(3),
      k2: undefined,
    }
    // deepFreeze(item3) // no, Dao is expected to mutate object to add id, created, updated
    const expected = { ...item3 }
    delete expected.k2

    await dao.save(item3)

    expected.updated = item3.updated // as it's mutated

    const item3Loaded = await dao.requireById(item3.id)
    expectMatch([expected], [item3Loaded], quirks)
    expect(item3Loaded.k2).toBeUndefined()
    expect(Object.keys(item3Loaded)).not.toContain('k2')
    expect(Object.keys(item3)).toContain('k2')
    expect(item3.k2).toBeUndefined()
  })

  test('saveBatch test items', async () => {
    const itemsSaved = await dao.saveBatch(items)
    expect(itemsSaved[0]).toBe(items[0]) // expect "same object" returned

    // no unnecessary mutation
    const { updated: _, ...clone } = itemsClone[0]!
    expect(items[0]).toMatchObject(clone)

    expectMatch(expectedItems, itemsSaved, quirks)
  })

  // GET not empty
  test('getByIds all items', async () => {
    const rows = await dao.getByIds(items.map(i => i.id).concat('abcd'))
    expectMatch(
      expectedItems,
      _sortBy(rows, r => r.id),
      quirks,
    )
  })

  // QUERY
  if (support.queries) {
    test('runQuery(all) should return all items', async () => {
      let rows = await dao.query().runQuery()
      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    if (support.dbQueryFilter) {
      test('query even=true', async () => {
        let rows = await dao.query().filter('even', '==', true).runQuery()
        rows = _sortBy(rows, r => r.id)
        expectMatch(
          expectedItems.filter(i => i.even),
          rows,
          quirks,
        )
      })
    }

    if (support.dbQueryOrder) {
      test('query order by k1 desc', async () => {
        const rows = await dao.query().order('k1', true).runQuery()
        expectMatch([...expectedItems].reverse(), rows, quirks)
      })
    }

    if (support.dbQuerySelectFields) {
      test('projection query with only ids', async () => {
        let rows = await dao.query().select(['id']).runQuery()
        rows = _sortBy(rows, r => r.id)
        expectMatch(
          expectedItems.map(item => _pick(item, ['id'])),
          rows,
          quirks,
        )
      })
    }

    test('runQueryCount should return 3', async () => {
      expect(await dao.query().runQueryCount()).toBe(3)
    })
  }

  // STREAM
  if (support.streaming) {
    test('streamQueryForEach all', async () => {
      let rows: TestItemBM[] = []
      await dao.query().streamQueryForEach(bm => void rows.push(bm))

      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    test('streamQuery all', async () => {
      let rows: TestItemBM[] = await dao.query().streamQuery().toArray()

      rows = _sortBy(rows, r => r.id)
      expectMatch(expectedItems, rows, quirks)
    })

    test('streamQueryIdsForEach all', async () => {
      let ids: string[] = []
      await dao.query().streamQueryIdsForEach(id => void ids.push(id))
      ids = ids.sort()
      expectMatch(
        expectedItems.map(i => i.id),
        ids,
        quirks,
      )
    })

    test('streamQueryIds all', async () => {
      let ids = await dao.query().streamQueryIds().toArray()
      ids = ids.sort()
      expectMatch(
        expectedItems.map(i => i.id),
        ids,
        quirks,
      )
    })

    test('streamSaveTransform', async () => {
      const items2 = createTestItemsBM(2).map(i => ({ ...i, id: i.id + '_str' }))
      const ids = items2.map(i => i.id)

      await _pipeline([Readable.from(items2), ...dao.streamSaveTransform()])

      const items2Loaded = await dao.getByIds(ids)
      expectMatch(items2, items2Loaded, quirks)

      // cleanup
      await dao.query().filterIn('id', ids).deleteByQuery()
    })
  }

  // DELETE BY
  if (support.queries) {
    test('deleteByQuery even=false', async () => {
      const deleted = await dao.query().filter('even', '==', false).deleteByQuery()
      expect(deleted).toBe(items.filter(item => !item.even).length)
      expect(await dao.query().runQueryCount()).toBe(1)
    })

    test('cleanup', async () => {
      // CLEAN UP
      await dao.query().deleteByQuery()
    })
  }

  if (support.transactions) {
    test('transaction happy path', async () => {
      // cleanup
      await dao.query().deleteByQuery()

      // Test that id, created, updated are created
      const now = nowUnix()

      await dao.runInTransaction(async tx => {
        const row = _omit(item1, ['id', 'created', 'updated'])
        await tx.save(dao, row)
      })

      const loaded = await dao.query().runQuery()
      expect(loaded.length).toBe(1)
      expect(loaded[0]!.id).toBeDefined()
      expect(loaded[0]!.created).toBeGreaterThanOrEqual(now)
      expect(loaded[0]!.updated).toBe(loaded[0]!.created)

      await dao.runInTransaction(async tx => {
        await tx.deleteById(dao, loaded[0]!.id)
      })

      // saveBatch [item1, 2, 3]
      // save item3 with k1: k1_mod
      // delete item2
      // remaining: item1, item3_with_k1_mod
      await dao.runInTransaction(async tx => {
        await tx.saveBatch(dao, items)
        await tx.save(dao, { ...items[2]!, k1: 'k1_mod' })
        await tx.deleteById(dao, items[1]!.id)
      })

      const rows = await dao.query().runQuery()
      const expected = [items[0], { ...items[2]!, k1: 'k1_mod' }]
      expectMatch(expected, rows, quirks)
    })

    test('transaction rollback', async () => {
      await expect(
        dao.runInTransaction(async tx => {
          await tx.deleteById(dao, items[2]!.id)
          await tx.save(dao, { ...items[0]!, k1: 5 as any }) // it should fail here
        }),
      ).rejects.toThrow()

      const rows = await dao.query().runQuery()
      const expected = [items[0], { ...items[2]!, k1: 'k1_mod' }]
      expectMatch(expected, rows, quirks)
    })

    if (support.queries) {
      test('transaction cleanup', async () => {
        await dao.query().deleteByQuery()
      })
    }
  }
}
