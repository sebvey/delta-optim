from dataclasses import dataclass
from pathlib import Path


BASE_PATH = Path("tmp/optim")

@dataclass(init=False)
class TableConf:
    name: str
    path: Path

    def __init__(self,name: str):
        self.name = name
        self.path = BASE_PATH / name

RAW_TABLE_CONF = TableConf("raw")

COMPACT_DELTARS_CONF = TableConf("compact_deltars")
COMPACT_RW_CONF = TableConf("compact_rw")
COMPACT_DELTAIO_CONF = TableConf("compact_deltaio")
