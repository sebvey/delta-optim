# CONSTRUCTING RAW TABLE ...
# Number of parquet files: 546
# Total size of table: 749.15

# CONSTRUCTING COMPACT TABLE ...
# Number of parquet files: 12
# Total size of table: 637.21

# CONSTRUCTING ZORDERED TABLE ...
# Number of parquet files: 6
# Total size of table: 581.89

# QUERY STATS:
# - Query        : duckdb_query
# - Iterations   : 100
# - Table        : raw
# => Duration (s): 4.1331
┌──────────┬────────────┬────────────────────┬─────────┐
│ location │ year_month │     mean_value     │ row_cnt │
│ varchar  │  varchar   │       double       │  int64  │
├──────────┼────────────┼────────────────────┼─────────┤
│ Lyon     │ 202401     │ 0.5998257254106444 │ 2143960 │
│ Lyon     │ 202402     │ 0.5998704322531426 │ 2004841 │
│ Lyon     │ 202403     │ 0.5999028015824874 │ 2143245 │
│ Lyon     │ 202404     │ 0.6001155573534254 │ 2073231 │
│ Lyon     │ 202405     │ 0.6000932302743356 │ 2141642 │
│ Lyon     │ 202406     │ 0.6004101070393202 │ 2074512 │
└──────────┴────────────┴────────────────────┴─────────┘


# QUERY STATS:
# - Query        : duckdb_query
# - Iterations   : 100
# - Table        : compact
# => Duration (s): 0.7662
┌──────────┬────────────┬────────────────────┬─────────┐
│ location │ year_month │     mean_value     │ row_cnt │
│ varchar  │  varchar   │       double       │  int64  │
├──────────┼────────────┼────────────────────┼─────────┤
│ Lyon     │ 202401     │ 0.5998257254106502 │ 2143960 │
│ Lyon     │ 202402     │ 0.5998704322531435 │ 2004841 │
│ Lyon     │ 202403     │ 0.5999028015824895 │ 2143245 │
│ Lyon     │ 202404     │ 0.6001155573534263 │ 2073231 │
│ Lyon     │ 202405     │ 0.6000932302743462 │ 2141642 │
│ Lyon     │ 202406     │ 0.6004101070393185 │ 2074512 │
└──────────┴────────────┴────────────────────┴─────────┘


# QUERY STATS:
# - Query        : duckdb_query
# - Iterations   : 100
# - Table        : zordered
# => Duration (s): 0.846
┌──────────┬────────────┬────────────────────┬─────────┐
│ location │ year_month │     mean_value     │ row_cnt │
│ varchar  │  varchar   │       double       │  int64  │
├──────────┼────────────┼────────────────────┼─────────┤
│ Lyon     │ 202401     │  0.599825725410648 │ 2143960 │
│ Lyon     │ 202402     │ 0.5998704322531492 │ 2004841 │
│ Lyon     │ 202403     │ 0.5999028015824875 │ 2143245 │
│ Lyon     │ 202404     │ 0.6001155573534138 │ 2073231 │
│ Lyon     │ 202405     │ 0.6000932302743426 │ 2141642 │
│ Lyon     │ 202406     │ 0.6004101070393277 │ 2074512 │
└──────────┴────────────┴────────────────────┴─────────┘

### FINAL STATS FOR duckdb_query
# COMPACT/RAW: 19.0 %
# ZORDERED/RAW: 20.0%

# QUERY STATS:
# - Query        : polars_eager_query
# - Iterations   : 100
# - Table        : raw
# => Duration (s): 165.0047
shape: (6, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ len     │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ u32     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.599826   ┆ 2143960 │
│ Lyon     ┆ 202402     ┆ 0.59987    ┆ 2004841 │
│ Lyon     ┆ 202403     ┆ 0.599903   ┆ 2143245 │
│ Lyon     ┆ 202404     ┆ 0.600116   ┆ 2073231 │
│ Lyon     ┆ 202405     ┆ 0.600093   ┆ 2141642 │
│ Lyon     ┆ 202406     ┆ 0.60041    ┆ 2074512 │
└──────────┴────────────┴────────────┴─────────┘

# QUERY STATS:
# - Query        : polars_eager_query
# - Iterations   : 100
# - Table        : compact
# => Duration (s): 132.8193
shape: (6, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ len     │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ u32     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.599826   ┆ 2143960 │
│ Lyon     ┆ 202402     ┆ 0.59987    ┆ 2004841 │
│ Lyon     ┆ 202403     ┆ 0.599903   ┆ 2143245 │
│ Lyon     ┆ 202404     ┆ 0.600116   ┆ 2073231 │
│ Lyon     ┆ 202405     ┆ 0.600093   ┆ 2141642 │
│ Lyon     ┆ 202406     ┆ 0.60041    ┆ 2074512 │
└──────────┴────────────┴────────────┴─────────┘

# QUERY STATS:
# - Query        : polars_eager_query
# - Iterations   : 100
# - Table        : zordered
# => Duration (s): 143.6674
shape: (6, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ len     │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ u32     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.599826   ┆ 2143960 │
│ Lyon     ┆ 202402     ┆ 0.59987    ┆ 2004841 │
│ Lyon     ┆ 202403     ┆ 0.599903   ┆ 2143245 │
│ Lyon     ┆ 202404     ┆ 0.600116   ┆ 2073231 │
│ Lyon     ┆ 202405     ┆ 0.600093   ┆ 2141642 │
│ Lyon     ┆ 202406     ┆ 0.60041    ┆ 2074512 │
└──────────┴────────────┴────────────┴─────────┘
### FINAL STATS FOR polars_eager_query
# COMPACT/RAW: 80.0 %
# ZORDERED/RAW: 87.0%

# QUERY STATS:
# - Query        : polars_lazy_query
# - Iterations   : 100
# - Table        : raw
# => Duration (s): 81.3353
shape: (6, 4)
┌──────────┬────────────┬────────────┬─────────┐
│ location ┆ year_month ┆ mean_value ┆ len     │
│ ---      ┆ ---        ┆ ---        ┆ ---     │
│ str      ┆ str        ┆ f64        ┆ u32     │
╞══════════╪════════════╪════════════╪═════════╡
│ Lyon     ┆ 202401     ┆ 0.599826   ┆ 2143960 │
│ Lyon     ┆ 202402     ┆ 0.59987    ┆ 2004841 │
│ Lyon     ┆ 202403     ┆ 0.599903   ┆ 2143245 │
│ Lyon     ┆ 202404     ┆ 0.600116   ┆ 2073231 │
│ Lyon     ┆ 202405     ┆ 0.600093   ┆ 2141642 │
│ Lyon     ┆ 202406     ┆ 0.60041    ┆ 2074512 │
└──────────┴────────────┴────────────┴─────────┘

Traceback (most recent call last):
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/.venv/bin/delta-optim", line 8, in <module>
    sys.exit(main())
             ~~~~^^
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/src/delta_optim/__init__.py", line 51, in main
    .map(srv.query.query_stats(benchmark_conf.iter_nbr)(query))
     ~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/.venv/lib/python3.13/site-packages/xfp/xlist.py", line 127, in map
    return Xlist([f(el) for el in self])
                  ~^^^^
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/src/delta_optim/srv/query.py", line 64, in query_stats
    duration = timeit.timeit(
        lambda: query(table),
        number=iter_nbr)
  File "/usr/local/Cellar/python@3.13/3.13.0_1/Frameworks/Python.framework/Versions/3.13/lib/python3.13/timeit.py", line 237, in timeit
    return Timer(stmt, setup, timer, globals).timeit(number)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^
  File "/usr/local/Cellar/python@3.13/3.13.0_1/Frameworks/Python.framework/Versions/3.13/lib/python3.13/timeit.py", line 180, in timeit
    timing = self.inner(it, self.timer)
  File "<timeit-src>", line 6, in inner
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/src/delta_optim/srv/query.py", line 65, in <lambda>
    lambda: query(table),
            ~~~~~^^^^^^^
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/src/delta_optim/srv/query.py", line 39, in polars_lazy_query
    return pl.scan_delta(table).pipe(_polars_base_query).collect()
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^
  File "/Users/ippon/code/workspaces/sebvey/python-prj/delta-optim/.venv/lib/python3.13/site-packages/polars/lazyframe/frame.py", line 2029, in collect
    return wrap_df(ldf.collect(callback))
                   ~~~~~~~~~~~^^^^^^^^^^
polars.exceptions.ShapeError: unable to vstack, column names don't match: "location" and "value"
