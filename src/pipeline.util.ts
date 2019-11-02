import { Mapper } from '@naturalcycles/js-lib'
import {
  _pipeline,
  ReadableTyped,
  transformBuffer,
  transformMap,
  TransformMapOptions,
} from '@naturalcycles/nodejs-lib'
import { CommonDB } from './common.db'
import { CommonDBSaveOptions, SavedDBEntity } from './db.model'

export interface DBPipelineOptions<IN extends SavedDBEntity, OUT extends SavedDBEntity>
  extends CommonDBSaveOptions,
    TransformMapOptions<OUT> {
  input: ReadableTyped<IN>
  output: CommonDB

  /**
   * Table to save to
   */
  table: string

  /**
   * @default 100
   *
   * Determines the size of .saveBatch()
   */
  batchSize?: number
}

const passthroughMapper: Mapper<any, any> = item => item

/**
 * Similar to stream.pipeline()
 * Pipes stream from input DBQuery into output CommonDB (running .saveBatch() for all records).
 * Handles backpressure.
 * Allows to define a mapper to map object between input and output.
 */
export async function dbPipeline<IN extends SavedDBEntity = any, OUT extends SavedDBEntity = IN>(
  opt: DBPipelineOptions<IN, OUT>,
  mapper: Mapper<IN, OUT> = passthroughMapper,
): Promise<void> {
  const { batchSize = 100, output, table } = opt

  await _pipeline([
    opt.input,
    transformMap(mapper, opt),
    transformBuffer<OUT>({ batchSize }),
    transformMap<OUT[], void>(async dbms => {
      await output.saveBatch(table, dbms, opt)
    }),
  ])
}
