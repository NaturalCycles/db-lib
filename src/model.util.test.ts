import { deepFreeze, mockTime } from '@naturalcycles/test-lib'
import { assignIdCreatedUpdated } from './model.util'

beforeEach(() => {
  jest.restoreAllMocks()
  mockTime()
  jest.spyOn(require('@naturalcycles/nodejs-lib'), 'stringId').mockImplementation(() => 'someId')
})

test('assignIdCreatedUpdated', () => {
  const o = {
    id: '123',
    updated: 123,
    created: 123,
  }

  deepFreeze(o)

  // Should preserve
  expect(assignIdCreatedUpdated(o, true)).toEqual(o)
  expect(assignIdCreatedUpdated(o)).toMatchSnapshot()
})
