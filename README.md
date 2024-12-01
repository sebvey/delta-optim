
GLOBALLY:
- could be nice to compare between delta and pure parquet

WITH DUCKDB:
- mean value slightly differs between iterations
- Result seems WAY FASTER than Polars, it seems that when the Relation
  is not collected, an estimation? is done from stats ?!?
- first iter stats is way slower

-> NO USE TO ZORDER... IT'S THE OPPOSITE, SLIGHTY INCREASE QUERIES

WITH POLARS:
- eager is veeerrrry slow compared to duckdb
- lazy crashes (it's experimental)
