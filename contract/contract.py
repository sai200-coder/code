# Databricks notebook source
# DBTITLE 1,Notebook Documentation: SQL/Parameter Externalization
# Code converted on 2026-02-06 16:52:21
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql.functions import lit, to_timestamp, when, expr, col, explode
from databricks_conversion_supplements import DatabricksConversionSupplements

# COMMAND ----------

# DBTITLE 1,Load external params and queries
import json
import importlib.util
import sys

# Load parameters from JSON
#replace /dbfs/Users/sai.work0488@gmail.com/dim_contract_params.json with relative path 
with open('/dbfs/Users/sai.work0488@gmail.com/dim_contract_params.json', 'r') as f:
    params = json.load(f)

# Dynamically load queries.py module
# replace '/dbfs/Users/sai.work0488@gmail.com/queries.py
spec = importlib.util.spec_from_file_location('queries', '/dbfs/Users/sai.work0488@gmail.com/queries.py')
queries = importlib.util.module_from_spec(spec)
sys.modules['queries'] = queries
spec.loader.exec_module(queries)


# COMMAND ----------

# DBTITLE 1,Widgets using params dict defaults
dbutils.widgets.text('APT_CONFIG_FILE', params.get('APT_CONFIG_FILE', '$PROJDEF'))
params['APT_CONFIG_FILE'] = dbutils.widgets.get('APT_CONFIG_FILE')

dbutils.widgets.text('ps_Company', params.get('ps_Company', ''))
params['ps_Company'] = dbutils.widgets.get('ps_Company')

dbutils.widgets.text('ps_FDM', params.get('ps_FDM', ''))
params['ps_FDM'] = dbutils.widgets.get('ps_FDM')

dbutils.widgets.text('ps_Common', params.get('ps_Common', ''))
params['ps_Common'] = dbutils.widgets.get('ps_Common')

dbutils.widgets.text('ps_Staging_StagingDataSource', params.get('ps_Staging_StagingDataSource', ''))
params['ps_Staging_StagingDataSource'] = dbutils.widgets.get('ps_Staging_StagingDataSource')

dbutils.widgets.text('ps_Staging_StagingUserName', params.get('ps_Staging_StagingUserName', ''))
params['ps_Staging_StagingUserName'] = dbutils.widgets.get('ps_Staging_StagingUserName')

dbutils.widgets.text('ps_Staging_StagingPassword', params.get('ps_Staging_StagingPassword', ''))
params['ps_Staging_StagingPassword'] = dbutils.widgets.get('ps_Staging_StagingPassword')

dbutils.widgets.text('ps_Staging_StagingDatabase', params.get('ps_Staging_StagingDatabase', ''))
params['ps_Staging_StagingDatabase'] = dbutils.widgets.get('ps_Staging_StagingDatabase')

dbutils.widgets.text('ps_Staging_StagingSchema', params.get('ps_Staging_StagingSchema', ''))
params['ps_Staging_StagingSchema'] = dbutils.widgets.get('ps_Staging_StagingSchema')

# COMMAND ----------

# DBTITLE 1,Drop target table (externalized)
#before running this file ,please uncomment spark.sql and comment print statement
#spark.sql(queries.DROP_DIM_RESOLUTION_CONTRACT_STG.format(**params))
print(queries.DROP_DIM_RESOLUTION_CONTRACT_STG.format(**params))

# COMMAND ----------

# DBTITLE 1,Create target table (externalized and parameterized)
#before running this file ,please uncomment spark.sql and comment print statement
#spark.sql(queries.CREATE_DIM_RESOLUTION_CONTRACT_STG.format(**params))
print(queries.CREATE_DIM_RESOLUTION_CONTRACT_STG.format(**params))

# COMMAND ----------

# DBTITLE 1,Create target table (staging and keys logic)
# Component src_dummy, Type Pre SQL 
# spark.sql(f"""CREATE CLUSTERED COLUMNSTORE INDEX cci_DIM_RESOLUTION_CONTRACT_STG ON TEMP_TABLE_ps_Staging.StagingDatabase#.wrbWRK.DIM_RESOLUTION_CONTRACT_STG""")\\\\
    
# clarify of having just the delta table is enough becuase delta table is already clustered columnstore mean its columnar design
# do we need optimise zorder if there are heavy loads on the specific columns or just leave it as it is

# COMMAND ----------

# DBTITLE 1,Create src_dummy temp view (externalized)
#before running this file ,please uncomment spark.sql and comment print statement
#spark.sql(queries.CREATE_SRC_DUMMY_VIEW)
print(queries.CREATE_SRC_DUMMY_VIEW)

# COMMAND ----------

# DBTITLE 1,Create cp_Trash temp view (externalized)
#before running this file ,please uncomment spark.sql and comment print statement
#spark.sql(queries.CREATE_CP_TRASH_VIEW)
print(queries.CREATE_CP_TRASH_VIEW)

# COMMAND ----------

# DBTITLE 1,Select final dim_resolution_contract_stg (externalized)
#before running this file ,please uncomment spark.sql and comment print statement
df_result = spark.sql(queries.SELECT_DIM_RESOLUTION_CONTRACT_STG.format(**params))
display(df_result)

# COMMAND ----------

