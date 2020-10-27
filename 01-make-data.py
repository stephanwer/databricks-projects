# Databricks notebook source
from glob import glob
import random
import json

from faker import Faker

f = Faker("de_de")

def next_order(order_id):
  print(order_id, end=" ")
  name = "%s %s" % (f.first_name(), f.last_name())
  street = f.street_address()
  city = f.city()
  state = f.state()
  postcode = f.postcode()

  order = {
    "order_id": order_id,
    "name": name,
    "sku": random.randint(10000, 100000),
    "price": round(random.random() * 100, 2),
    "shipTo": {"name": name, "address": street, "city": city, "state": state, "zip": postcode},
    "billTo": {"name": name, "address": street, "city": city, "state": state, "zip": postcode},
  }
  if random.random() < 0.2:
    choice = random.randint(0,5)
    print(f"==> created error {choice}")
    if choice == 0:
      order["sku"] = random.randint(1000, 10000)
    elif choice == 1:
      del order["billTo"]
    elif choice == 2:
      del order["shipTo"]
    elif choice == 3:
      order["billTo"]["zip"] = order["billTo"]["zip"][:4]
    elif choice == 4:
      order["price"] = -order["price"]
    elif choice == 5:
      order["sku"] = str(order["sku"])
  else:
    print()
  return order

# COMMAND ----------

random.seed(42)
order_id = 10005
output_path = "s3a://oetrta/stephan/json-raw/"
dbutils.fs.rm(output_path, True)
dbutils.fs.mkdirs(output_path)

# COMMAND ----------


import gzip
import time 
for i in range(100):
  order_id += i
  od = next_order(order_id)
  json_str = json.dumps(od)
  json_bytes = json_str.encode('utf-8')           
  with gzip.GzipFile(f"/dbfs/mnt/oetrta/stephan/json-raw/order_{order_id}.gz", 'w') as fil:
     fil.write(json_bytes)       
  time.sleep(2)