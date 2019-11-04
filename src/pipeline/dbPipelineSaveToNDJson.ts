import { ErrorMode, Mapper, passthroughMapper, pMap } from '@naturalcycles/js-lib'
import {
  boldWhite,
  dimWhite,
  grey,
  NDJsonStats,
  pipelineToNDJsonFile,
  transformLogProgress,
  transformMap,
  TransformMapOptions,
  TransformToNDJsonOptions,
  yellow,
} from '@naturalcycles/nodejs-lib'
import * as fs from 'fs-extra'
import { ZlibOptions } from 'zlib'
import { CommonDB } from '../common.db'
import { DBQuery } from '../index'

export interface DBPipelineSaveToNDJsonOptions extends TransformToNDJsonOptions {
  /**
   * DB to dump data from.
   */
  db: CommonDB

  /**
   * List of tables to dump. If undefined - will call CommonDB.getTables() and dump ALL tables returned.
   */
  tables?: string[]

  /**
   * How many tables to dump in parallel.
   * @default 16
   * Set to `1` for serial (1 at a time) processing or debugging.
   */
  concurrency?: number

  /**
   * @default ErrorMode.SUPPRESS
   *
   * Used in high-level pMap(tables, ...)
   * Also used as default option for TransformMapOptions
   */
  errorMode?: ErrorMode

  /**
   * @default undefined
   * If set - will dump maximum that number of rows per table
   */
  limit?: number

  /**
   * Directory path to store dumped files. Will create `${tableName}.jsonl` (or .jsonl.gz if gzip=true) files.
   * All parent directories will be created.
   * @default to process.cwd()
   */
  outputDirPath?: string

  /**
   * @default false
   * If true - will fail if output file already exists.
   */
  protectFromOverwrite?: boolean

  /**
   * @default true
   */
  gzip?: boolean

  /**
   * Only applicable if `gzip` is enabled
   */
  zlibOptions?: ZlibOptions

  /**
   * Optionally you can provide mapper that is going to run for each table.
   * @default `{}`
   * Default mappers will be "passthroughMapper" (pass all data as-is).
   */
  mapperPerTable?: Record<string, Mapper>

  /**
   * You can alter default `transformMapOptions` here.
   * @default (see the code)
   * The goal to have default values that are reasonable for such a job to provide resilient output (forgiving individual errors).
   * `metric` will be set to table name
   */
  transformMapOptions?: TransformMapOptions
}

// const log = Debug('nc:db-lib:pipeline')

/**
 * Pipeline from input stream(s) to a NDJSON file (optionally gzipped).
 * File is overwritten (by default).
 * Input stream can be a stream from CommonDB.streamQuery()
 * Allows to define a mapper and a predicate to map/filter objects between input and output.
 * Handles backpressure.
 *
 * Optionally you can provide mapperPerTable and @param transformMapOptions (one for all mappers) - it will run for each table.
 */
export async function dbPipelineSaveToNDJson(
  opt: DBPipelineSaveToNDJsonOptions,
): Promise<NDJsonStats> {
  const {
    db,
    concurrency = 16,
    limit = 0,
    outputDirPath = process.cwd(),
    protectFromOverwrite = false,
    zlibOptions,
    mapperPerTable = {},
    transformMapOptions,
    errorMode = ErrorMode.SUPPRESS,
  } = opt
  const gzip = opt.gzip !== false // default to true

  let { tables } = opt

  console.log(`>> ${dimWhite('dbPipelineSaveToNDJson')} started in ${grey(outputDirPath)}...`)

  await fs.ensureDir(outputDirPath)

  if (!tables) {
    tables = await db.getTables()
  }

  console.log(`${yellow(tables.length)} ${boldWhite('table(s)')}:\n\n` + tables.join('\n') + '\n')

  const statsPerTable: Record<string, NDJsonStats> = {}

  await pMap(
    tables,
    async table => {
      const stream = db.streamQuery(new DBQuery(table).limit(limit))

      const filePath = `${outputDirPath}/${table}.jsonl` + (gzip ? '.gz' : '')

      const stats = await pipelineToNDJsonFile(
        [
          stream,
          transformLogProgress({
            metric: table,
            logEvery: 1000,
          }),
          transformMap(mapperPerTable[table] || passthroughMapper, {
            errorMode,
            ...transformMapOptions,
            metric: table,
          }),
        ],
        {
          filePath,
          protectFromOverwrite,
          gzip: true,
          zlibOptions,
          strict: false,
          sortObjects: true,
          ...opt,
        },
      )

      statsPerTable[table] = stats
    },
    { concurrency, errorMode },
  )

  const statsTotal = NDJsonStats.createCombined(Object.values(statsPerTable))

  console.log(statsTotal.toPretty('total'))

  return statsTotal
}
