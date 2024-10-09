# Databricks notebook source
# DBTITLE 1,Importações
import requests
from pyspark.sql.functions import col
import time

# COMMAND ----------

# DBTITLE 1,Parâmetros e variáveis
api_url = "https://api.openbrewerydb.org/v1/breweries"
max_retries = 3
retry_delay = 5
num_per_page = 200

layer = "bronze"
file_name = "breweries"
datalake_path = f"abfss://datalake-bees@datalakebees.dfs.core.windows.net/{layer}/{file_name}"

print(datalake_path)

# COMMAND ----------

# DBTITLE 1,Funções
# requests and retries
def get_data(url, max_retries, retry_delay):
    retries = max_retries
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # verifica se o status é 200
            data = response.json()
            if data:  # dados não estão vazios
                return {"data": data, "status": "success"}
            else:
                return {"data": None, "status": "empty_response"}
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(retry_delay)
            else:
                return {"data": None, "status": f"error: {str(e)}"}  # erro após esgotar tentativas

# extract data
def extract_data(api_url, num_per_page, max_retries, retry_delay):
    all_data = []
    page = 1
    status = "success"
    
    while True:
        url = f"{api_url}?page={page}&per_page={num_per_page}"
        result = get_data(url, max_retries, retry_delay)
        if result["status"] == "success" and result["data"]:
            all_data.extend(result["data"])
            page += 1
        elif result["status"] == "empty_response":
            print(f"No more data at page {page}.")
            if page == 1: status = "empty_response"
            break  # finaliza se a resposta estiver vazia
        else:
            status = result["status"]  # status de erro
            print(f"Error: {status}")
            break  # interrompe se houver um erro

    return {"data": all_data, "status": status}


# COMMAND ----------

# DBTITLE 1,Extração dos dados
result = extract_data(api_url, num_per_page, max_retries, retry_delay)

# COMMAND ----------

# DBTITLE 1,Gravação
isDataFilled = result["status"] == "success" and len(result["data"]) > 0
isDataEmpty = result["status"] == "empty_response"

if isDataFilled:
    df = spark.createDataFrame(result["data"])
    df.write\
        .format("json")\
        .mode("overwrite")\
        .save(f"{datalake_path}")
    print(f"Data extracted and saved successfully: {len(result['data'])} records.")
elif isDataEmpty:
    print("No data found in the API.")
else:
    print(f"Failed to extract data: {result['status']}")

# COMMAND ----------

# DBTITLE 1,Saída notebook
exit_status = f'"status": "{result["status"]}"'
open_obj = '{'
close_obj = '}'
dbutils.notebook.exit(f'{open_obj}{exit_status}{close_obj}')
