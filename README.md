TODO:
- separate build data from use data
- build with duckdb if possible (random functions) and compare


GLOBALLY:
- could be nice to compare between delta and pure parquet

WITH DUCKDB:
- mean value slightly differs between iterations
- Result seems WAY FASTER than Polars: it seems that when iterating, things are cached by duckdb !
- first iter stats is way slower

-> NO USE TO ZORDER... IT'S THE OPPOSITE, SLIGHTY INCREASE QUERIES

WITH POLARS:
- it crashes ...
