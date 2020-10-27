# Databricks notebook source
# MAGIC %md # Validation pipeline from S3 to Kafka using delta and AutoLoader

# COMMAND ----------

# DBTITLE 1,The demo setup
import base64 

def display_img(img_path, width_percent=100):
  with open(img_path, "rb") as img_file:
    b64_data = base64.b64encode(img_file.read()).decode('utf-8') 
  displayHTML(f"""<img src="data:image/png;base64,{b64_data}" width="{width_percent}%">""") 
  
display_img("/dbfs/home/bernhard/pipeline.png", 80)

# COMMAND ----------

# MAGIC %md ## Prepare filesystem

# COMMAND ----------

input_path =              "s3a://oetrta/stephan/json-raw/"

result_path =             "s3a://oetrta/stephan/orders.delta"
checkpoint_path =         "s3a://oetrta/stephan/orders.chk"

correct_result_path =     "s3a://oetrta/stephan/correct_orders.delta"
correct_checkpoint_path = "s3a://oetrta/stephan/correct_orders.chk"

kafka_checkpoint_path =   "s3a://oetrta/stephan/kafka.chk"

# COMMAND ----------

import time

#dbutils.fs.rm(input_path, True)
dbutils.fs.rm(result_path, True)
dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(correct_result_path, True)
dbutils.fs.rm(correct_checkpoint_path, True)
dbutils.fs.rm(kafka_checkpoint_path, True)

time.sleep(2)
#dbutils.fs.mkdirs(input_path)

# COMMAND ----------

# DBTITLE 1,Check the input path
dbutils.fs.ls(input_path)

# COMMAND ----------

# MAGIC %md ## Create tables
# MAGIC ### Silver table orders_validated

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists stephan.orders_validated

# COMMAND ----------

spark.sql(f"""
create table stephan.orders_validated (
  order_id int,
  valid boolean,
  name string,
  price double,
  sku int,
  billTo_address string,
  billTo_city string ,
  billTo_name string,
  billTo_state string,
  billTo_zip string,
  shipTo_address string,
  shipTo_city string,
  shipTo_name string,
  shipTo_state string,  
  shipTo_zip string,
  
  order string,
  error string
)
USING DELTA 
PARTITIONED BY (valid)
LOCATION '{result_path}'
""")

# COMMAND ----------

# MAGIC %md ### Gold table orders_final

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists stephan.orders_final;

# COMMAND ----------

spark.sql(f"""
CREATE TABLE stephan.orders_final (
  order_id int,
  name string,
  price double,
  sku int,
  shipTo_address string,
  shipTo_city string,
  shipTo_name string,
  shipTo_state string,
  shipTo_zip string,
  billTo_address string,
  billTo_city string,
  billTo_name string,
  billTo_state string,
  billTo_zip string
) 
USING DELTA 
LOCATION '{correct_result_path}'
""")

# COMMAND ----------

# MAGIC %md ## Schemas
# MAGIC 
# MAGIC ### Create a schema for Structured Streaming
# MAGIC 
# MAGIC Order Example: 
# MAGIC ```json
# MAGIC {
# MAGIC     "order_id": 1,
# MAGIC     "name": "Irma Reuter",
# MAGIC     "sku": 32685,
# MAGIC     "price": 74.08,
# MAGIC     "shipTo": {
# MAGIC         "name": "Irma Reuter",
# MAGIC         "address": "Koch IIstr. 193",
# MAGIC         "city": "Döbeln",
# MAGIC         "state": "Thüringen",
# MAGIC         "zip": "11136"
# MAGIC     },
# MAGIC     "billTo": {
# MAGIC         "name": "Irma Reuter",
# MAGIC         "address": "Koch IIstr. 193",
# MAGIC         "city": "Döbeln",
# MAGIC         "state": "Thüringen",
# MAGIC         "zip": "11136"
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC Note: This is mandatory, but not sufficient for schema enforcement

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
from delta.tables import DeltaTable
import pandas as pd

#
# For autoloader, load json string into one column
#
input_sparkschema = T.StructType([
  T.StructField("order", T.StringType())
])

#
# For parsing the validated JSON String with Spark
#
def address_schema(tag):
  return T.StructField(tag, T.StructType([
    T.StructField("address", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("zip", T.StringType())
  ]))

order_sparkschema = T.StructType([
  T.StructField("order_id", T.IntegerType()),
  T.StructField("name", T.StringType()),
  T.StructField("price", T.DoubleType()),
  T.StructField("sku", T.IntegerType()),
  address_schema("shipTo"),
  address_schema("billTo"), 
])

# COMMAND ----------

# MAGIC %md ### Json Schema Validation
# MAGIC 
# MAGIC - `shipTo` can be `null`, it will be the same as `billTo`
# MAGIC - `price` needs to be a float and > 0
# MAGIC - `sku` needs to have only numbers and being larger than 20000
# MAGIC - `plz` needs to be a string of digits and of length 5

# COMMAND ----------

from fastjsonschema import compile, JsonSchemaException
import json

#
# JSON SChema for validation of orders
#
order_schema = {
  "$schema": "http://json-schema.org/draft-07/schema#",
    
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "name":    { "type": "string" },
        "address": { "type": "string" },
        "city":    { "type": "string" },
        "state":   { "type": "string" },
        "zip":     { "type": "string", "pattern": "[0-9]{5}" }
      },
      "required": ["name", "address", "city", "zip"],
      "additionalProperties": False
    }
  },

  "type": "object",
  "properties": {
    "order_id": {"type": "integer", "minimum": 0},
    "name": { "type": "string" },
    "price": {"type": "number", "minimum": 0},
    "sku": {"type": "integer", "minimum": 10000},
    "billTo": { "$ref": "#/definitions/address" },
    "shipTo": { "$ref": "#/definitions/address" }
  },
  "required": ["name", "price", "sku", "billTo"],
  "additionalProperties": False
}

validate_order = compile(order_schema)

def parse_validate_order(order_str):
    try:
        validate_order(json.loads(order_str))
        result = ""
    except JsonSchemaException as ex:
        result = "Invalid schema: {}: {}".format(ex.message, ex.value)
    except Exception as ex:
        result = "Generic error: {}".format(str(ex))
    return result

@F.pandas_udf(T.StringType())
def parse_validate(order: pd.Series) -> pd.Series:
    return order.map(parse_validate_order)

# COMMAND ----------

# DB
dapid961e738c87ad9f8179670f4d83fc08e
# Git
ab840db86760a9e22c69975c1ce4127804803fcc

# COMMAND ----------

# MAGIC %md ## Streaming ingest
# MAGIC 
# MAGIC ### Starting the input stream with AutoLoader

# COMMAND ----------

options = {
  "cloudFiles.format": "text",
  "cloudFiles.region": "us-west-2",
  "cloudFiles.includeExistingFiles": "true",
}

file_format = "cloudFiles"

input_df = (
  spark
  .readStream
  .format("text")
  # missing permissions
  #.format("cloudFiles")
  #.option("cloudFiles.format", "text")
  #.option("cloudFiles.region", "us-west-2")
  #.option("cloudFiles.includeExistingFiles", "true")
  .schema(input_sparkschema)
  .load(path=input_path)
)

trigger_once = False
if trigger_once:
  trigger_conf = {"once": True}
else:
  trigger_conf = {"processingTime": "5 seconds"}

# COMMAND ----------

# MAGIC %md ### Flattening Json and validating schema
# MAGIC 
# MAGIC A new column `valid` will contain whether schema is ok or not (result being partitioned by `valid`)

# COMMAND ----------

def if_valid(col):
    return F.when(F.col("valid")==True, col)

valid_df = (input_df
      .withColumn("error", parse_validate("order"))
      .withColumn("valid", F.col("error") == "")
      .withColumn("parsed_order", if_valid(F.from_json("order", order_sparkschema)))
)

for col in ["order_id", "name", "price", "sku"]:
    valid_df = valid_df.withColumn(col, F.col(f"parsed_order.{col}"))
    
for col in ["billTo", "shipTo"]:
    for attr in ["name", "address", "city", "state", "zip"]:
        valid_df = valid_df.withColumn(f"{col}_{attr}", F.col(f"parsed_order.{col}.{attr}"))
        
valid_stream = (valid_df
  .select([
    "order_id", "valid", 
    "name", "price", "sku", 
    "billTo_address", "billTo_city", "billTo_name", "billTo_state", "billTo_zip", 
    "shipTo_address", "shipTo_city", "shipTo_name", "shipTo_state", "shipTo_zip", 
    "order", "error"])
  .writeStream
  .partitionBy("valid")
  .trigger(**trigger_conf)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_path)
  .start(result_path)
)

# COMMAND ----------

# MAGIC %md ## Streaming to the gold table

# COMMAND ----------

def upsertToDelta(microBatchDF, batchId): 
  microBatchDF.createOrReplaceTempView("updates")

  # TODO select latest entry of same key
  
  microBatchDF._jdf.sparkSession().sql("""
    MERGE INTO stephan.orders_final t
    USING updates s
    ON s.order_id = t.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

if trigger_once:
  valid_stream.awaitTermination() # only for trigger once

result_stream = (
  spark
  .readStream
  .format("delta")
  .load(result_path)
  .filter(F.col("valid") == True)
  .select(
    "order_id", "name", "price", "sku",
    "shipTo_address", "shipTo_city", "shipTo_name", "shipTo_state", "shipTo_zip", 
    "billTo_address", "billTo_city", "billTo_name", "billTo_state", "billTo_zip"
  )
)

(result_stream
  .writeStream
  .trigger(**trigger_conf)
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .option("checkpointLocation", correct_checkpoint_path)
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from stephan.orders_final

# COMMAND ----------

# MAGIC %md ## Streaming to Kafka

# COMMAND ----------

kafka_bootstrap_servers = dbutils.secrets.get("oetrta", "kafka-bootstrap-servers") #TLS

TOPIC = "stephan-order-out"

(result_stream
  .select(
    F.col("order_id").cast("string").alias("key"), 
    F.to_json(F.struct(
      F.col("order_id"), F.col("name"), F.col("price"), F.col("sku"),
      F.col("shipTo_address"), F.col("shipTo_city"), F.col("shipTo_name"), F.col("shipTo_state"), F.col("shipTo_zip"), 
      F.col("billTo_address"), F.col("billTo_city"), F.col("billTo_name"), F.col("billTo_state"), F.col("billTo_zip")      
    )).alias("value"))
  .writeStream
  .trigger(**trigger_conf)
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
  .option("kafka.security.protocol", "SSL") 
  .option("checkpointLocation", kafka_checkpoint_path)
  .option("topic", TOPIC)
  .start()
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Results

# COMMAND ----------

# DBTITLE 1,The good
# MAGIC %sql
# MAGIC select order_id, name, price, sku, 
# MAGIC   billTo_address, billTo_city, billTo_name, billTo_state, billTo_zip,
# MAGIC   shipTo_address, shipTo_city, shipTo_name, shipTo_state, shipTo_zip
# MAGIC from stephan.orders_validated 
# MAGIC where valid = true

# COMMAND ----------

# DBTITLE 1,The bad
# MAGIC %sql
# MAGIC select order, error
# MAGIC from stephan.orders_validated 
# MAGIC where valid=false

# COMMAND ----------

