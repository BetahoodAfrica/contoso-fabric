# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import re

WS = notebookutils.runtime.context["currentWorkspaceName"]

def onelake(lakehouse: str, sub: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/{sub}"

def snake(name: str) -> str:
    # SalesOrderID -> sales_order_id
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
