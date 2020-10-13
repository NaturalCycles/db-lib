import { ErrorMode, _filterNullishValues } from '@naturalcycles/js-lib'
import { CommonSchema, CommonSchemaField, DATA_TYPE } from './common.schema'

export interface CommonSchemaGeneratorCfg {
  /**
   * Name of the Table
   */
  table: string

  /**
   * @default SUPPRESS
   *
   * On error (field has multiple types):
   * if ErrorMode.THROW_IMMEDIATE - will throw on .add()
   * if ErrorMode.THROW_AGGREGATED - will throw on .generate()
   */
  errorMode?: ErrorMode

  /**
   * @default false
   * If true - fields will be sorted by name alphabetically (also inside objects)
   */
  sortedFields?: boolean
}

const LOCAL_DATE_PATTERN = new RegExp(/[0-9]{4}-[01][0-9]-[0-3][0-9]/)

/**
 * Class that helps to generate CommonSchema by processing ALL rows through it.
 */
export class CommonSchemaGenerator<ROW = any> {
  constructor(public cfg: CommonSchemaGeneratorCfg) {}

  private fieldByName: Record<string, CommonSchemaField> = {}
  private nullableFields = new Set<string>()
  // private fieldsWithMultipleTypes: Record<string, DATA_TYPE[]> = {}

  add(row: ROW): void {
    if (!row) return // safety

    Object.entries(row).forEach(([fieldName, value]) => {
      this.fieldByName[fieldName] = this.mergeFields(
        this.fieldByName[fieldName],
        this.detectType(fieldName, value),
      )

      if (this.fieldByName[fieldName].type === DATA_TYPE.NULL) {
        this.nullableFields.add(fieldName)
      }
    })

    // missing fields
    Object.keys(this.fieldByName).forEach(fieldName => {
      if (!(fieldName in row)) {
        // missing field!
        this.nullableFields.add(fieldName)
      }
    })
  }

  private mergeFields(
    existing: CommonSchemaField | undefined,
    newField: CommonSchemaField,
  ): CommonSchemaField {
    if (!existing) return newField

    if (newField.type === DATA_TYPE.UNKNOWN || newField.type === DATA_TYPE.NULL) {
      return {
        ...existing,
      }
    }

    if (existing.type === DATA_TYPE.UNKNOWN || existing.type === DATA_TYPE.NULL) {
      return {
        ...newField,
      }
    }

    let type = existing.type

    if (existing.type !== newField.type) {
      const [type1, type2] = [existing.type, newField.type].sort()
      if (type1 === DATA_TYPE.FLOAT && type2 === DATA_TYPE.INT) {
        type = DATA_TYPE.FLOAT
      } else if (type1 === DATA_TYPE.LOCAL_DATE && type2 === DATA_TYPE.STRING) {
        type = DATA_TYPE.STRING
      } else {
        // Type mismatch! Oj!
        return newField // currently just use "latest" type
      }
    }

    // Type is same
    const minLen =
      existing.minLen !== undefined
        ? Math.min(...[existing.minLen, newField.minLen!].filter(Boolean))
        : undefined
    const maxLen =
      existing.maxLen !== undefined
        ? Math.max(...[existing.maxLen, newField.maxLen!].filter(Boolean))
        : undefined

    // todo: recursively merge/compare array/object schemas

    return _filterNullishValues({
      ...newField,
      type,
      minLen,
      maxLen,
    })
  }

  private detectType(name: string, value: any, level = 1): CommonSchemaField {
    // Null
    if (value === undefined || value === null) {
      return {
        name,
        type: DATA_TYPE.NULL,
      }
    }

    // String
    if (typeof value === 'string') {
      // LocalDate
      if (LOCAL_DATE_PATTERN.test(value)) {
        return { name, type: DATA_TYPE.LOCAL_DATE }
      }

      return { name, type: DATA_TYPE.STRING, minLen: value.length, maxLen: value.length }
    }

    // Int, Float
    if (typeof value === 'number') {
      // Cannot really detect TIMESTAMP
      return {
        name,
        type: Number.isInteger(value) ? DATA_TYPE.INT : DATA_TYPE.FLOAT,
        minLen: value,
        maxLen: value,
      }
    }

    // Boolean
    if (typeof value === 'boolean') {
      return { name, type: DATA_TYPE.BOOLEAN }
    }

    // Binary
    if (Buffer.isBuffer(value)) {
      return { name, type: DATA_TYPE.BINARY, minLen: value.length, maxLen: value.length }
    }

    // Array
    if (Array.isArray(value)) {
      return {
        name,
        type: DATA_TYPE.ARRAY,
        arrayOf: this.detectType('', value[0], level + 1),
      }
    }

    // Object
    if (typeof value === 'object') {
      return {
        name,
        type: DATA_TYPE.OBJECT,
        objectFields: Object.entries(value).map(([k, v]) => this.detectType(k, v, level + 1)),
      }
    }

    return { name, type: DATA_TYPE.UNKNOWN }
  }

  generate(): CommonSchema<ROW> {
    // set nullability
    Object.keys(this.fieldByName).forEach(fieldName => {
      if (!this.nullableFields.has(fieldName)) {
        this.fieldByName[fieldName].notNull = true
      }
    })

    const { table, sortedFields } = this.cfg

    const fieldNames = Object.keys(this.fieldByName)
    if (sortedFields) fieldNames.sort() // mutates

    // todo: sort object fields too

    return {
      table,
      fields: fieldNames.map(name => this.fieldByName[name]),
    }
  }

  static generateFromRows<ROW>(cfg: CommonSchemaGeneratorCfg, rows: ROW[] = []): CommonSchema<ROW> {
    const gen = new CommonSchemaGenerator(cfg)
    rows.forEach(r => gen.add(r))
    return gen.generate()
  }
}
