from .constant import RAW_TABLE_CONF

from . import srv


def main():

    print("@@@ Spark Local Init ...")
    spark = srv.spark_session.get_local_session()
    print("@@@ Spark Local Init DONE")

    raw_table = srv.table.rs.get(RAW_TABLE_CONF)
    print("# RAW TABLE STATS:")
    srv.utils.print_stats(raw_table)

    compact_table = srv.compact.with_deltars(raw_table)
    srv.utils.print_stats(compact_table)

    compact_table = srv.compact.with_deltaio(spark,RAW_TABLE_CONF)
    srv.utils.print_stats(compact_table)

