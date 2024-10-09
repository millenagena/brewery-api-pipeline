# Databricks notebook source
# DBTITLE 1,Importações
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# COMMAND ----------

# DBTITLE 1,Funções
def tableExists(catalog, schema, table):
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        return tables.filter(col("tableName") == table).count() > 0
    except:
        return False

def tableComments(full_table_name, description, comments):
    # description
    spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{description}'")
    
    # comments
    for column, comment in comments.items():
        spark.sql(f"ALTER TABLE {full_table_name} CHANGE COLUMN {column} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,Parâmetros e variáveis
catalog = "gold"

# silver
silver_table = "silver.breweries.brewery_type_location"

# silver
schema = "breweries"
table_name = "brewery_type_by_state"
full_table_name = f"{catalog}.{schema}.{table_name}"
flagTableExists = tableExists(catalog, schema, table_name)

print(f"silver_table: {silver_table}")
print(f"table_name: {full_table_name}")
print(flagTableExists)

# COMMAND ----------

# DBTITLE 1,Leitura da origem
silver_df = spark.table(silver_table)

# COMMAND ----------

# DBTITLE 1,Agregação
# count number of breweries by type and state
gold_df = silver_df.groupBy("state", "brewery_type").count()

# control columns
gold_df = gold_df.withColumn('load_date', F.current_date())
gold_df = gold_df.withColumn('load_datetime', F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Gravação e otimização
# full
gold_df.write\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .clusterBy("brewery_type", "state")\
    .mode("overwrite")\
    .saveAsTable(f"{full_table_name}")
print(f"Carga full executada com sucesso na tabela {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Comentários
table_description = "Table containing the count of breweries by type and state."
column_comments = {
    "state": "State where the brewery is located",
    "brewery_type": "Type of brewery",
    "count": "Number of breweries by type and state",
    "load_date": "Data load date",
    "load_datetime": "Data load date and time"
}

tableComments(full_table_name, table_description, column_comments)
