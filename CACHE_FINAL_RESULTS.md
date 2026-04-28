# Cache Port: Final Performance Results

## Test Setup

- **Dataset**: ClickBench hits.parquet, 1,000,000 rows, 31 numeric columns, 100 parquet files
- **Query**: `SELECT COUNT(*) FROM hits WHERE "RegionID" > 100`
- **Runs**: 20 queries per test, fresh ingest each run

## Side-by-Side: Query Latency (ms)

| Q | With Cache | Without Cache |
|---|-----------|---------------|
| 1 | 140 | 211 |
| 2 | 130 | 159 |
| 3 | 135 | 155 |
| 4 | 132 | 156 |
| 5 | 150 | 157 |
| 6 | 133 | 152 |
| 7 | 130 | 159 |
| 8 | 145 | 152 |
| 9 | 132 | 154 |
| 10 | 130 | 156 |
| 11 | 134 | 154 |
| 12 | 137 | 151 |
| 13 | 133 | 160 |
| 14 | 130 | 156 |
| 15 | 138 | 153 |
| 16 | 131 | 154 |
| 17 | 134 | 153 |
| 18 | 133 | 158 |
| 19 | 138 | 155 |
| 20 | 129 | 151 |
| **Avg** | **135** | **158** |
| **Avg (Q2+)** | **134** | **155** |
| **Min** | **129** | **151** |
| **Max** | **150** | **211** |

## Cache Stats

| Metric | With Cache | Without Cache |
|--------|-----------|---------------|
| Metadata HITs | 60,060 | 0 |
| Metadata MISSes | 0 | 0 |
| Statistics HITs | 20,000 | 0 |
| Statistics MISSes | 0 | 0 |
| Hit rate | 100.0% | N/A |
| New log lines | 100,260 | 160 |

## Summary

| Metric | With Cache | Without Cache | Improvement |
|--------|-----------|---------------|-------------|
| Avg (Q2+) | 134 ms | 155 ms | **21 ms (14% faster)** |
| Q1 (cold) | 140 ms | 211 ms | **71 ms (34% faster)** |
| Min | 129 ms | 151 ms | **22 ms (15% faster)** |
