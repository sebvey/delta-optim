from deltalake import Field, Schema
from deltalake.schema import PrimitiveType

# deltalake 'bug'
# nullable=False on timestamp -> Invariants in writer feature missing
# https://github.com/delta-io/delta-rs/issues/2882

schema = Schema([
    Field("year_month",PrimitiveType("string"),nullable=True),
    Field("datetime",PrimitiveType("timestamp_ntz"),nullable=True),
    Field("location", PrimitiveType("string"),nullable=True),
    Field("value",PrimitiveType("double"),nullable=True),
])
