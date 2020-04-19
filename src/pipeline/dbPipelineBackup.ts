import { AppError, ErrorMode, Mapper, pMap, _passthroughMapper } from '@naturalcycles/js-lib'
import {
  NDJsonStats,
  transformLogProgress,
  TransformLogProgressOptions,
  transformMap,
  TransformMapOptions,
  transformTap,
  transformToNDJson,
  _pipeline,
} from '@naturalcycles/nodejs-lib'
import { boldWhite, dimWhite, grey, yellow } from '@naturalcycles/nodejs-lib/dist/colors'
import { dayjs } from '@naturalcycles/time-lib'
import * as fs from 'fs-extra'
import { createGzip, ZlibOptions } from 'zlib'
import { CommonDB } from '../common.db'
import { DBQuery } from '../index'
import { CommonSchemaGenerator } from '../schema/commonSchemaGenerator'

export interface DBPipelineBackupOptions extends TransformLogProgressOptions {
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
   * If set - will do "incremental backup" (not full), only for entities that updated >= `sinceUpdated`
   * @default undefined
   */
  sinceUpdated?: number

  /**
   * Directory path to store dumped files. Will create `${tableName}.ndjson` (or .ndjson.gz if gzip=true) files.
   * All parent directories will be created.
   * @default to process.cwd()
   */
  outputDirPath: string

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

  /**
   * @default false
   * If true - will use CommonSchemaGenerator to detect schema from input data.
   */
  emitSchemaFromData?: boolean

  /**
   * @default false
   * If true - will use CommonDB.getTableSchema() and emit schema.
   */
  emitSchemaFromDB?: boolean
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
export async function dbPipelineBackup(opt: DBPipelineBackupOptions): Promise<NDJsonStats> {
  const {
    db,
    concurrency = 16,
    limit = 0,
    sinceUpdated,
    outputDirPath,
    protectFromOverwrite = false,
    zlibOptions,
    mapperPerTable = {},
    transformMapOptions,
    errorMode = ErrorMode.SUPPRESS,
    emitSchemaFromDB = false,
    emitSchemaFromData = false,
  } = opt
  const strict = errorMode !== ErrorMode.SUPPRESS
  const gzip = opt.gzip !== false // default to true

  let { tables } = opt

  const sinceUpdatedStr = sinceUpdated ? ' since ' + grey(dayjs.unix(sinceUpdated).toPretty()) : ''

  console.log(
    `>> ${dimWhite('dbPipelineBackup')} started in ${grey(outputDirPath)}...${sinceUpdatedStr}`,
  )

  await fs.ensureDir(outputDirPath)

  if (!tables) {
    tables = await db.getTables()
  }

  console.log(`${yellow(tables.length)} ${boldWhite('table(s)')}:\n` + tables.join('\n'))

  const statsPerTable: Record<string, NDJsonStats> = {}

  await pMap(
    tables,
    async table => {
      let q = new DBQuery(table).limit(limit)

      if (sinceUpdated) {
        q = q.filter('updated', '>=', sinceUpdated)
      }

      const filePath = `${outputDirPath}/${table}.ndjson` + (gzip ? '.gz' : '')
      const schemaFilePath = `${outputDirPath}/${table}.schema.json`

      if (protectFromOverwrite && (await fs.pathExists(filePath))) {
        throw new AppError(`dbPipelineBackup: output file exists: ${filePath}`)
      }

      const started = Date.now()
      let rows = 0

      await fs.ensureFile(filePath)

      console.log(`>> ${grey(filePath)} started...`)

      if (emitSchemaFromDB) {
        const schema = await db.getTableSchema(table)
        await fs.writeJson(schemaFilePath, schema, { spaces: 2 })
        console.log(`>> ${grey(schemaFilePath)} saved (generated from DB)`)
      }

      const schemaGen = emitSchemaFromData
        ? new CommonSchemaGenerator({ table, sortedFields: true })
        : undefined

      await _pipeline([
        db.streamQuery(q),
        transformLogProgress({
          logEvery: 1000,
          ...opt,
          metric: table,
        }),
        transformMap(mapperPerTable[table] || _passthroughMapper, {
          errorMode,
          flattenArrayOutput: true,
          ...transformMapOptions,
          metric: table,
        }),
        transformTap(row => {
          rows++
          if (schemaGen) schemaGen.add(row)
        }),
        transformToNDJson({ strict, sortObjects: true }),
        ...(gzip ? [createGzip(zlibOptions)] : []), // optional gzip
        fs.createWriteStream(filePath),
      ])

      const { size: sizeBytes } = await fs.stat(filePath)

      const stats = NDJsonStats.create({
        tookMillis: Date.now() - started,
        rows,
        sizeBytes,
      })

      console.log(`>> ${grey(filePath)}\n` + stats.toPretty())

      if (schemaGen) {
        const schema = schemaGen.generate()
        await fs.writeJson(schemaFilePath, schema, { spaces: 2 })
        console.log(`>> ${grey(schemaFilePath)} saved (generated from data)`)
      }

      statsPerTable[table] = stats
    },
    { concurrency, errorMode },
  )

  const statsTotal = NDJsonStats.createCombined(Object.values(statsPerTable))

  console.log(statsTotal.toPretty('total'))

  return statsTotal
}
