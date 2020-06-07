

###### TEDx-Load-Aggregate-Model-Watch-Next version
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

#### filter TEDx dataset on url field
tedx_dataset_filtered = tedx_dataset.filter("url like 'http%'")

## READ TAGS DATASET
tags_dataset_path = "s3://unibg-tedx-data-1063244/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset_filtered.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
# attenzione, la parte di modifica dell'etichetta idx va fatta una sola volta prima di caricare su mongo DB, è inutile farla qui
tedx_dataset_agg.printSchema()


####  WATCH NEXT:

#### READ WATCH NEXT DATASET
watch_next_path = "s3://unibg-tedx-data-1063244/watch_next_dataset.csv"
wn_dataset = spark.read.option("header","true").csv(watch_next_path)

#### DROPPING DUPLICATE
#wn_count = wn_dataset.count()
#print(f"Pre drop duplicated: {wn_count}")
wn_dataset = wn_dataset.dropDuplicates()
#print(f"After drop duplicated: {wn_dataset.count()}")
#print(f"Dropped: {wn_count-wn_dataset.count()}")

#### FILTER BAD URL
wn_dataset = wn_dataset.filter("url not like 'https://www.ted.com/session/new?%'")

# CREATE THE AGGREGATE MODEL, ADD WATCH NEXT TO TEDX_DATASET
wn_dataset_agg = wn_dataset.groupBy(col("idx").alias("idx_wn")).agg(collect_list("watch_next_idx").alias("watch_next_ids"))
wn_dataset_agg.printSchema()
tedx_dataset_aggf = tedx_dataset_agg.join(wn_dataset_agg, tedx_dataset_agg.idx == wn_dataset_agg.idx_wn, "left") \
    .drop("idx_wn") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx")

tedx_dataset_aggf.printSchema()


#### CONNECT WITH MONGODB
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
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_aggf, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
