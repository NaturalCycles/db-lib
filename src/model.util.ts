import { stringId } from '@naturalcycles/nodejs-lib'
import { CreatedUpdated, CreatedUpdatedId, ObjectWithId } from './db.model'

export function createdUpdatedFields(existingObject?: CreatedUpdated): CreatedUpdated {
  const now = Math.floor(Date.now() / 1000)
  return {
    created: (existingObject && existingObject.created) || now,
    updated: now,
  }
}

export function createdUpdatedIdFields(existingObject?: CreatedUpdatedId): CreatedUpdatedId {
  const now = Math.floor(Date.now() / 1000)
  return {
    created: (existingObject && existingObject.created) || now,
    id: (existingObject && existingObject.id) || stringId(),
    updated: now,
  }
}

export function idField(existingObject?: CreatedUpdatedId): ObjectWithId {
  return {
    id: (existingObject && existingObject.id) || stringId(),
  }
}

export function deserializeJsonField<T = any>(f?: string): T {
  return JSON.parse(f || '{}')
}

export function serializeJsonField(f: any): string | undefined {
  if (f === undefined) return
  return JSON.stringify(f)
}
