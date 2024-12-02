from dataclasses import dataclass
import timeit
from typing import Any, Callable

from deltalake import DeltaTable
import toolz

from ...constant import TableConf
from .. import table


@dataclass
class BenchmarkConf:
        iter_nbr: int
        queries: list[Callable[[DeltaTable], Any]]

@toolz.curry
def query_stats(iter_nbr: int, query: Any,table_conf: TableConf) -> float:

    duration = timeit.timeit(
        lambda: query(table_conf),
        number=iter_nbr)
    result = query(table_conf)

    delta_table = table.rs.get(table_conf)

    print()
    print(f"# QUERY STATS:")
    print(f"# - Query        : {query.__name__}")
    print(f"# - Iterations   : {iter_nbr}")
    print(f"# - Table        : {table_conf.name}")
    print(f"# => Duration (s): {round(duration,4)}")
    print(result)

    return duration
