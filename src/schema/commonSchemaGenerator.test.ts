import { createTestItemsDBM, getTestItemSchema, TEST_TABLE } from '../testing'
import { CommonSchemaGenerator } from './commonSchemaGenerator'

test('testItems', () => {
  const rows = createTestItemsDBM(5)
  const schema = CommonSchemaGenerator.generateFromRows({ table: TEST_TABLE }, rows)
  // console.log(schema)
  expect(schema).toMatchSnapshot()
})

test('complex example', () => {
  const rows = [
    {
      k1: 'v1',
      k3: 'v3',
      k4: -45.123,
      k5: 45,
      k6: false,
      k7: '1984-06-21',
      k8: 15224232342,
      k9: [],
      k10: ['a', 'b', 'c'],
      k11: ['a', 'b', 45],
      k12: ['a', 'b', 45, null],
      k20: {
        a: 'a',
        b: 34,
        c: null,
      },
    },
    {
      k1: 'v2',
      k4: 42.123,
      k5: 45,
      k6: true,
      k8: 15224232342,
      k9: [],
      k11: ['a', 'b', 45, null],
      k20: {
        b: 37,
      },
    },
  ]

  const schema = CommonSchemaGenerator.generateFromRows({ table: TEST_TABLE }, rows)
  // console.log(JSON.stringify(schema, null,2))
  expect(schema).toMatchSnapshot()
})

test('getTestItemSchema', () => {
  const schema = getTestItemSchema()
  // console.log(schema)
  expect(schema).toMatchSnapshot()
})
