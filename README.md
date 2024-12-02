# INSTALL
- install uv: https://docs.astral.sh/uv/getting-started/installation/
- at root folder of the project, sync uv: `uv sync`
- activate the virtual env: `source ./venv/bin/activate`


# RUN

/!\ The data folder is set in the `src/delta_optim/constant.py` file  
/!\ default is `<cwd>/tmp/optim`  
/!\ a year of data is about 2GB, delete it once you're done.


Script files are in `src/delta_optim/`.  
First, create table, then compact, and last query, in that order !


## Create table script (`job_create_delta.py`):  
run the command `create-delta`  

It will create a delta table with fake data, given parameters set (hard) in the script:
- one 'delta_write' by day between `start_date` and `end_date`
- partitioned by locations (locations value in script)
- a parquet file for a day + location is ~1.5MB


## Compact table script (`job_compact_delta.py`):
run the command `compact-delta`  

It will clone the table folder and compact the delta table using:
- pyspark + deltaio/delta -> "compact_deltaio" folder
- deltaio/deltars -> "compact_deltars" folder

## Query script (`job_compact_delta.py`):
run the command `query-delta`  

The queries to compare and the tables to query are set in the script.  

Queries among:
- `spark_query`: pyspark with a local spark session
- `duckdb_query`: 'lazy' duckdb query (delta_scan used)
- `polars_eager_query`: 'eager' polars query (read_delta used)
- `polars_lazy_query`: 'lazy polars query (scan_delta used)

Tables among:
- RAW_TABLE_CONF: the table initially produced, without compaction
- COMPACT_DELTAIO_CONF: the table compacted by deltaio/delta using spark
- COMPACT_DELTARS_CONF: the table compacted by deltaio/deltars
