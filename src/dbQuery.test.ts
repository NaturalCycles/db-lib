import { DBQuery } from './dbQuery'

test('DBQuery', () => {
  const q = new DBQuery('TestKind')
  expect(q.table).toBe('TestKind')
})
