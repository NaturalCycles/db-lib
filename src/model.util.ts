import { CreatedUpdated, CreatedUpdatedId } from '@naturalcycles/js-lib'
import { stringId } from '@naturalcycles/nodejs-lib'

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
  existingObject?: Partial<CreatedUpdatedId<string>> | null,
): CreatedUpdatedId<string> {
  const now = Math.floor(Date.now() / 1000)
  return {
    created: existingObject?.created || now,
    id: existingObject?.id || stringId(),
    updated: now,
  }
}

export function deserializeJsonField<T = any>(f?: string): T {
  return JSON.parse(f || '{}')
}

export function serializeJsonField(f: any): string | undefined {
  if (f === undefined) return
  return JSON.stringify(f)
}
