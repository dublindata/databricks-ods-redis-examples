# Databricks notebook source
# MAGIC %md
# MAGIC # Building a An Azure Redis Cache using Spark
# MAGIC
# MAGIC We can easily build our Redis cache from an existing UC/Hive Metastore table by first reading in our data (and in our case, for demo purposes, sampling it for our POC), then using a Dataframe to persist the data as a hash, that we can then pull down later from our consuming systems.
# MAGIC
# MAGIC This sample uses the Redis Spark connector (https://github.com/RedisLabs/spark-redis) and is setting the connection via the cluster spark configuration.
# MAGIC
# MAGIC Once the data is built, we can refresh our cache using SaveMode Append to update the cache with new rows, and add changes, by simply querying our Delta Lake tables.
# MAGIC
# MAGIC Let's see it in action!

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Read the source table
# MAGIC
# MAGIC We'll load up our orders from the sample TPCH database (~7.5M rows). Your data can be anything, but in my case I'm reading from a local hive_metastore location and reading in a table into a Spark dataframe called "orders."

# COMMAND ----------

orders = spark.read.table("hive_metastore.drewfurgiuele.tpch_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Sample the source table
# MAGIC
# MAGIC Since the demo uses the smaller SKU of Azure Redis, we'll sample our rows so it'll fit in memory. We'll take 15% of the rows (~1.1M rows)

# COMMAND ----------

sampled_orders = orders.sample(.15)

# COMMAND ----------

sampled_orders.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Initial Build of the Redis table/cache
# MAGIC
# MAGIC We'll use spark to overwrite (if it exists) the existing data in our target table and specify our hash key as the o_orderkey column. This can be a long process depending on the size of the cluster and amount of data to load. Note the simple code here: we're taking our sampled dataframe, overwriting the existing table (if it exists), specifying the format we're writing is Redis (hash tables), naming the table, and specifying our key column (that is, the key we'd look up in the cache).

# COMMAND ----------

sampled_orders.write.mode("overwrite").format("org.apache.spark.sql.redis").option("table", "tpch_orders").option("key.column", "o_orderkey").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update some records
# MAGIC
# MAGIC Now, let's update some records. We'll be using our record time column to track updates, but you can easily track updates by other methods, like time travel. While this example uses updates done manually, we could be ingesting changes from practically any source, even streaming sources to update the records and as part of those same pipelines, turn around and write our changes to Redis!
# MAGIC
# MAGIC Specifically, I'm looking at 10 rows in my dataset, and updating the "shipping priority column" and setting the last update time to the time of the change. These changes can be coming from anywhere, but for example's sake, let's just update the table. Then, we're using PySpark to a do a merge statement to update our original table with the dataframe that has our changes, and specifying what happens if our key exists and what happens if it doesn't. These changes will be persisted to the Delta Tables.

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, date_format, to_date, to_timestamp
from datetime import datetime
from delta.tables import *

last_update = '08-29-2023 23:00:00'

orders_to_update = [14922181,14922182,14922183,14922208,14922209,14922210,14922211,14922212,14922213,14922214]

rows_to_update = spark.read.table("hive_metastore.drewfurgiuele.tpch_orders").filter(col("o_orderkey").isin(orders_to_update))
new_values = rows_to_update.withColumn("o_shippriority",lit(0)).withColumn("last_updated", to_timestamp(lit(last_update), 'MM-dd-yyyy HH:mm:ss'))

deltaTableorders = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/drewfurgiuele.db/tpch_orders')

deltaTableorders.alias("orders").merge(
    source=new_values.alias("updates"),
    condition = "orders.o_orderkey = updates.o_orderkey"
  ).whenMatchedUpdate(set =
    {
      "o_shippriority": "updates.o_shippriority",
      "last_updated": "updates.last_updated"
    }
  ).whenNotMatchedInsert(values =
    {
      "o_custkey":"updates.o_custkey",
      "o_orderstatus":"updates.o_orderstatus",
      "o_totalprice":"updates.o_totalprice",
      "o_orderdate":"updates.o_orderdate",
      "o_orderpriority":"updates.o_orderpriority",
      "o_clerk":"updates.o_clerk",
      "o_shippriority":"updates.o_shippriority",
      "o_comment":"updates.o_comment",
      "last_updated": "updates.last_updated"
    }
  ).execute()



# COMMAND ----------

# MAGIC %md
# MAGIC # Verify our updates
# MAGIC
# MAGIC To make sure we actually updated our rows, we can then write a query that gets the recently updated rows and displays the results (in our example, there should be 10 rows)

# COMMAND ----------

last_update = '08/29/23 22:00:00'
datetime_object = datetime.strptime(last_update, '%m/%d/%y %H:%M:%S')

# COMMAND ----------

recent_updates = spark.read.table("hive_metastore.drewfurgiuele.tpch_orders").filter(col("last_updated") >= datetime_object )

# COMMAND ----------

display(recent_updates)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Update Redis Cache with Changes
# MAGIC
# MAGIC Now that we have the rows we've updated, we take ONLY those rows via the same dataframe that we used before, and then using the "append" mode of Spark which will either update an existing key (if it exists) or create a new record.

# COMMAND ----------

recent_updates.write.mode("append").format("org.apache.spark.sql.redis").option("table", "tpch_orders").option("key.column", "o_orderkey").save()
