# from deltalake import DeltaTable

# from ..domain import DuplicationMode
# from .table import duplicate


# def zordered_table(raw_table: DeltaTable, mode: DuplicationMode):

#     print()
#     print(f"# CONSTRUCTING ZORDERED TABLE ... ")

#     zordered_table = duplicate(raw_table,"zordered", mode)
#     zordered_table.optimize.z_order(columns=["location","value"])
#     zordered_table.vacuum(
#         retention_hours=0,
#         enforce_retention_duration=False,
#         dry_run=False
#     )

#     return zordered_table
