# QUERY STATS:
# - Query        : spark_query
# - Iterations   : 1
# - Table        : compact_deltars_rw
# => Duration (s): 10.4974
DataFrame[location: string, year_month: string, mean_value: double, row_cnt: bigint]

# QUERY STATS:
# - Query        : duckdb_query
# - Iterations   : 1
# - Table        : compact_deltars_rw
# => Duration (s): 1.5372
shape: (12, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ row_cnt │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ i64     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.600021   ┆ 2142453 │
│ Lyon     ┆ 202402     ┆ 0.600154   ┆ 2004117 │
│ Lyon     ┆ 202403     ┆ 0.600165   ┆ 2143774 │
│ Lyon     ┆ 202404     ┆ 0.599814   ┆ 2074442 │
│ Lyon     ┆ 202405     ┆ 0.600086   ┆ 2141892 │
│ …        ┆ …          ┆ …          ┆ …       │
│ Lyon     ┆ 202408     ┆ 0.599975   ┆ 2140951 │
│ Lyon     ┆ 202409     ┆ 0.599962   ┆ 2073493 │
│ Lyon     ┆ 202410     ┆ 0.600154   ┆ 2142083 │
│ Lyon     ┆ 202411     ┆ 0.600062   ┆ 2074282 │
│ Lyon     ┆ 202412     ┆ 0.600144   ┆ 2143214 │
└──────────┴────────────┴────────────┴─────────┘

# QUERY STATS:
# - Query        : polars_eager_query
# - Iterations   : 1
# - Table        : compact_deltars_rw
# => Duration (s): 9.5412
shape: (12, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ len     │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ u32     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.600021   ┆ 2142453 │
│ Lyon     ┆ 202402     ┆ 0.600154   ┆ 2004117 │
│ Lyon     ┆ 202403     ┆ 0.600165   ┆ 2143774 │
│ Lyon     ┆ 202404     ┆ 0.599814   ┆ 2074442 │
│ Lyon     ┆ 202405     ┆ 0.600086   ┆ 2141892 │
│ …        ┆ …          ┆ …          ┆ …       │
│ Lyon     ┆ 202408     ┆ 0.599975   ┆ 2140951 │
│ Lyon     ┆ 202409     ┆ 0.599962   ┆ 2073493 │
│ Lyon     ┆ 202410     ┆ 0.600154   ┆ 2142083 │
│ Lyon     ┆ 202411     ┆ 0.600062   ┆ 2074282 │
│ Lyon     ┆ 202412     ┆ 0.600144   ┆ 2143214 │
└──────────┴────────────┴────────────┴─────────┘
