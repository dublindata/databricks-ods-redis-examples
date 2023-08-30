-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Merging Changes from a DLT Table into a Lakehouse Table
-- MAGIC
-- MAGIC After our DLT pipeline runs, and ingests new data or changed data, we can use SQL commands to run a MERGE command to take the data in the Live table to write back to a table.

-- COMMAND ----------

MERGE INTO hive_metastore.drewfurgiuele.tpch_orders as orders
USING hive_metastore.drewfurgiuele.incoming_ods_changes as ods_changes
ON orders.o_orderkey = ods_changes.orderkey
WHEN MATCHED THEN
  UPDATE SET
    o_orderstatus = ods_changes.order_status,
    o_totalprice = ods_changes.total_price,
    o_orderdate = ods_changes.order_date,
    o_orderpriority = ods_changes.priority,
    o_clerk = ods_changes.clerk,
    o_shippriority = ods_changes.ship_priority,
    o_comment = ods_changes.comment,
    last_updated = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    last_updated
  )
  VALUES (
    ods_changes.orderkey,
    ods_changes.customer_key,
    ods_changes.order_status,
    ods_changes.total_price,
    ods_changes.order_date,
    ods_changes.priority,
    ods_changes.clerk,
    ods_changes.ship_priority,
    ods_changes.comment,
    current_timestamp()
  )    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Persisting the changes to Redis
-- MAGIC
-- MAGIC We can also use this table to get all our changes for Redis. We can either query the table above that now has our changes (by last updated time, for instance), or our same Delta Live table to get the changes into a dataframe.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC recent_changes = spark.read.table("hive_metastore.drewfurgiuele.incoming_ods_changes")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Modifing the shape of the data
-- MAGIC
-- MAGIC Since we used the incoming changes table and not the original table, we need to modify our column names. Notice how we used ANSI SQL in the first part of this notebook, but now we're using PySpark to change the column names.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC redis_changes = recent_changes.withColumnRenamed("orderkey","o_orderkey").withColumnRenamed("customer_key","o_custkey").withColumnRenamed("order_status","o_orderstatus").withColumnRenamed("total_price","o_totalprice").withColumnRenamed("order_date","o_orderdate").withColumnRenamed("priority","o_orderpriority").withColumnRenamed("clerk","o_clerk").withColumnRenamed("ship_priority","o_shippriority").withColumnRenamed("comment","o_comment")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Persist changes to Redis
-- MAGIC
-- MAGIC With our updated column names to match what we expect in Redis, with one line we update the cache

-- COMMAND ----------

-- MAGIC %python
-- MAGIC redis_changes.write.mode("append").format("org.apache.spark.sql.redis").option("table", "tpch_orders").option("key.column", "o_orderkey").save()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Building a Workflow
-- MAGIC
-- MAGIC Now that we have a process to processes incoming changes (with DLT) and a proceses that writes the changes to our Lakehouse tables and updates Redis, we can build a workflow to run on a schedule (or continuous) to grab changes, proces them, and update our Redis cache:
-- MAGIC
-- MAGIC ![Workflow](img/Workflow)
