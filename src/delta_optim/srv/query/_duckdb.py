import duckdb
import pyarrow as pa

from delta_optim.constant import TableConf

from .. import table


def duckdb_query(table_conf: TableConf) -> pa.Table:

    delta_table = table.rs.get(table_conf)
    return duckdb.sql(
        f"""
        SELECT
            location,
            year_month,
            mean(value) as 'mean_value',
            count(*) as 'row_cnt',
        FROM delta_scan('{delta_table.table_uri}')
        WHERE value > 0.2 and location = 'Lyon'
        GROUP BY location, year_month
        ORDER BY location, year_month
        """
    ).pl()
