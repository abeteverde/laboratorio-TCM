

###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join #funzioni di sql avanzate

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# librerie prese dalla documentazione di AWS


##### FROM FILES
tedx_dataset_path = "s3://unibg-tedx-data-1063244/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME']) #serve per passare dati in input, noi non lo vediamo per questa lezione

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
# le ultime due option servono per leggere il contenuto tra due apici come un blocco unico (evita di confondere le virgole all'interno del testo con dei separatori)
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://unibg-tedx-data-1063244/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \
#_id serve per dire a mongoDB di indicizzare secondo quel campo, altrimenti si inventa lui degli indici
tedx_dataset_agg.printSchema()




mongo_uri = "mongodb://cluster-tcm-shard-00-00-xiiyc.mongodb.net:27017,cluster-tcm-shard-00-01-xiiyc.mongodb.net:27017,cluster-tcm-shard-00-02-xiiyc.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx",
    "collection": "tedz_data",
    "username": "admin",
    "password": "xxx",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
