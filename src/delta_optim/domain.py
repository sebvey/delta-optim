from enum import Enum, auto
from deltalake import Field, Schema
from deltalake.schema import PrimitiveType

from .constant import BASE_PATH


schema = Schema([
    Field("year_month",PrimitiveType("string"),nullable=False),
    Field("datetime",PrimitiveType("timestamp_ntz"),nullable=False),
    Field("location", PrimitiveType("string"),nullable=False), # TODO - should be cardinal instead
    Field("value",PrimitiveType("double"),nullable=False),
])

class DuplicationMode(Enum):
    CloneFolder = auto()
    ReadWriteTable = auto()
