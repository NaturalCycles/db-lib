import { DBQuery } from './dbQuery'

test('DBQuery', () => {
  const q = new DBQuery('TestKind')
  expect(q.table).toBe('TestKind')
  expect(q.prettyConditions()).toEqual([])
})

test('prettyConditions', () => {
  const q = new DBQuery('TestKind').filter('a', '>', 5)
  expect(q.prettyConditions()).toEqual(['a>5'])
  expect(q.pretty()).toEqual('a>5')
})
