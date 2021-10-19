import { stringId } from '@naturalcycles/nodejs-lib'
import { CreatedUpdated, CreatedUpdatedId, ObjectWithId } from '@naturalcycles/js-lib'

export function createdUpdatedFields(
  existingObject?: Partial<CreatedUpdated> | null,
): CreatedUpdated {
  const now = Math.floor(Date.now() / 1000)
  return {
    created: existingObject?.created || now,
    updated: now,
  }
}

export function createdUpdatedIdFields(
  existingObject?: Partial<CreatedUpdatedId> | null,
): CreatedUpdatedId {
  const now = Math.floor(Date.now() / 1000)
  return {
    created: existingObject?.created || now,
    id: existingObject?.id || stringId(),
    updated: now,
  }
}

export function idField(existingObject?: Partial<CreatedUpdatedId> | null): ObjectWithId {
  return {
    id: existingObject?.id || stringId(),
  }
}

export function deserializeJsonField<T = any>(f?: string): T {
  return JSON.parse(f || '{}')
}

export function serializeJsonField(f: any): string | undefined {
  if (f === undefined) return
  return JSON.stringify(f)
}
