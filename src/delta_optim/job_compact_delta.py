from .constant import RAW_TABLE_CONF

from . import srv


def main():

    raw_table = srv.table.rs.get(RAW_TABLE_CONF)

    compact_table = srv.compact.with_deltars_compact(raw_table)
    srv.utils.print_stats(compact_table)

    compact_table = srv.compact.with_deltars_read_write(raw_table)
    srv.utils.print_stats(compact_table)
