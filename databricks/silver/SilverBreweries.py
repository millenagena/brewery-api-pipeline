# Databricks notebook source
# DBTITLE 1,Importação das bibliotecas
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from datetime import datetime, timedelta
from pyspark.sql.types import StringType, DoubleType, LongType

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
    spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ('comment' = '{description}')")

    # columns comments
    for column, comment in comments.items():
        spark.sql(f"ALTER TABLE {full_table_name} CHANGE COLUMN {column} COMMENT '{comment}'")

# COMMAND ----------

# DBTITLE 1,Parâmetros e variáveis
catalog = "silver"

# bronze
datalake_path = "abfss://datalake-bees@datalakebees.dfs.core.windows.net"
bronze_path = f"{datalake_path}/bronze/breweries"

# silver
schema = "breweries"
table_name = "brewery_type_location"
full_table_name = f"{catalog}.{schema}.{table_name}"
flagTableExists = tableExists(catalog, schema, table_name)

print(f"bronze_path: {bronze_path}")
print(f"table_name: {full_table_name}")
print(flagTableExists)

# COMMAND ----------

# DBTITLE 1,Leitura da origem
bronze_df = spark.read.json(bronze_path)

# COMMAND ----------

# DBTITLE 1,Query
silver_df = bronze_df.select(
    col("id").cast(StringType()),
    col("name").cast(StringType()),
    col("brewery_type").cast(StringType()),
    col("city").cast(StringType()),
    col("state").cast(StringType()),
    col("state_province").cast(StringType()),
    col("country").cast(StringType()),
    col("address_1").cast(StringType()),
    col("address_2").cast(StringType()),
    col("address_3").cast(StringType()),
    col("longitude").cast(DoubleType()),
    col("latitude").cast(DoubleType()),
    col("phone").cast(StringType()),
    col("postal_code").cast(StringType()),
    col("street").cast(StringType()),
    col("website_url").cast(StringType())
).filter(col("state").isNotNull())

# control columns
silver_df = silver_df.withColumn('load_date', F.current_date())
silver_df = silver_df.withColumn('load_datetime', F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Gravação da tabela
if flagTableExists:
    # merge
    silver_df.createOrReplaceTempView("upsert_silver")
    spark.sql(f"""
         merge into {full_table_name} as t
         using upsert_silver as s
         on t.id = s.id
         when matched then update set *
         when not matched then insert *
    """)
    print(f"Merge executado com sucesso na tabela {full_table_name}")
else:  
    # full
    silver_df.write\
        .format("delta")\
        .option("overwriteSchema", "true")\
        .partitionBy("state")\
        .mode("overwrite")\
        .saveAsTable(f"{full_table_name}")
    print(f"Carga full executada com sucesso na tabela {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Otimização
spark.sql(f"OPTIMIZE {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Comentários
if not flagTableExists:
    table_description = "This table contains detailed information about breweries including their location, contact information, and type."
    column_comments = {
        "id": "Unique identifier for the brewery",
        "name": "Name of the brewery",
        "brewery_type": "Type of the brewery",
        "city": "City where the brewery is located",
        "state": "State where the brewery is located",
        "state_province": "State or province where the brewery is located",
        "country": "Country where the brewery is located",
        "address_1": "Primary address of the brewery",
        "address_2": "Secondary address of the brewery",
        "address_3": "Tertiary address of the brewery",
        "longitude": "Longitude coordinate of the brewery",
        "latitude": "Latitude coordinate of the brewery",
        "phone": "Contact phone number of the brewery",
        "postal_code": "Postal code of the brewery",
        "street": "Street name of the brewery",
        "website_url": "Website URL of the brewery",
        "load_date": "Data load date",
        "load_datetime": "Data load date and time"
    }
    tableComments(full_table_name, table_description, column_comments)
