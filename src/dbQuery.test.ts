import { DBQuery } from './dbQuery'

test('DBQuery', () => {
  const q = new DBQuery('TestKind')
  expect(q.kind).toBe('TestKind')
})
