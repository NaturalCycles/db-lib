import { DBQuery } from '../dbQuery'
import {
  CommonTimeSeriesDaoCfg,
  TimeSeriesDataPoint,
  TimeSeriesDBM,
  TimeSeriesQuery,
} from './timeSeries.model'

/**
 * TimeSeries DB implemtation based on provided CommonDB database.
 * Turns any CommonDB database into TimeSeries DB. Kind of.
 */
export class CommonTimeSeriesDao {
  constructor(public cfg: CommonTimeSeriesDaoCfg) {}

  async ping(): Promise<void> {
    await this.cfg.db.ping()
  }

  async getSeries(): Promise<string[]> {
    return (await this.cfg.db.getTables())
      .map(t => /TIMESERIES_(.*)_RAW/.exec(t)?.[1] as string)
      .filter(Boolean)
  }

  async save(series: string, tsMillis: number, value: number): Promise<void> {
    await this.saveBatch(series, [[tsMillis, value]])
  }

  async saveBatch(series: string, dataPoints: TimeSeriesDataPoint[]): Promise<void> {
    if (!dataPoints.length) return

    const dbms: TimeSeriesDBM[] = dataPoints.map(([ts, v]) => ({
      id: ts,
      ts, // to allow querying by ts, since querying by id is not always available (Datastore is one example)
      v,
    }))

    await this.cfg.db.saveBatch(`TIMESERIES_${series}_RAW`, dbms as any)
  }

  async deleteById(series: string, tsMillis: number): Promise<void> {
    await this.deleteByIds(series, [tsMillis])
  }

  async deleteByIds(series: string, ids: number[]): Promise<void> {
    // Save to _RAW table with v=null
    await this.saveBatch(
      series,
      ids.map(id => [id, null]),
    )
  }

  async query(q: TimeSeriesQuery): Promise<TimeSeriesDataPoint[]> {
    const dbq = new DBQuery(`TIMESERIES_${q.series}_RAW`).order('ts')
    if (q.fromIncl) dbq.filter('ts', '>=', q.fromIncl)
    if (q.toExcl) dbq.filter('ts', '<', q.toExcl)

    const { records } = await this.cfg.db.runQuery<any, TimeSeriesDBM>(dbq)

    // todo: query from aggregated tables when step is above 'hour'

    return records
      .filter(r => r.v !== null && r.v !== undefined) // can be 0
      .map(r => [r.ts, r.v])
  }

  async optimize(): Promise<void> {
    // todo
  }
}
