import polars as pl

from .constant import RAW_TABLE_CONF
from . import srv


def main():

    print("@@@ Spark Local Init ...")
    spark = srv.spark_session.get_local_session()
    print("@@@ Spark Local Init DONE")

    raw_table = srv.table.rs.get(RAW_TABLE_CONF)
    print("# RAW TABLE STATS:")
    srv.utils.print_stats(raw_table)

    compact_rs_table = srv.compact.with_deltars()
    srv.utils.print_stats(compact_rs_table)

    (
        pl.DataFrame(compact_rs_table.get_add_actions())
        .select("path")
        .write_ndjson("tmp_COMPACT_RS_get_add_actions.json")
    )

    compact_io_table = srv.compact.with_deltaio(spark)
    srv.utils.print_stats(compact_io_table)
