# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ac0bf30b-2622-43e6-9198-d0905b2b34b5",
# META       "default_lakehouse_name": "Slow_variables",
# META       "default_lakehouse_workspace_id": "e9ab2e73-7dab-4a9a-83ba-4f0432562ce1",
# META       "known_lakehouses": [
# META         {
# META           "id": "ac0bf30b-2622-43e6-9198-d0905b2b34b5",
# META           "name": "Slow_variables"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run the below script or schedule it to run regularly to optimize your Lakehouse table 'dbo.Raw_data_lakehouse'

from delta.tables import *
deltaTable = DeltaTable.forName(spark, "dbo.Raw_data_lakehouse")
deltaTable.optimize().executeCompaction()

# If you only want to optimize a subset of your data, you can specify an optional partition predicate. For example:
#
#     from datetime import datetime, timedelta
#     startDate = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
#     deltaTable.optimize().where("date > '{}'".format(startDate)).executeCompaction()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
