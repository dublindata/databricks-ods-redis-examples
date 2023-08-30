-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Like Tables: Ingesting Changes via Auto Loader
-- MAGIC
-- MAGIC The Databricks Autoloader (https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/) is designed to ingest incoming files on cloud storage accounts to act as a streaming ingestion source. In this case, we'll be reading in multi-line JSON files that will be placed on our storage account, and appending the data to a "live" table that contains all the data that has been read before and the next time the table refreshes (either on a schedule, or via continuous updates)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE incoming_ods_changes
AS 
SELECT *, current_timestamp() as last_updated FROM cloud_files('abfss://odsexample@drewfurgiueledbx.dfs.core.windows.net/incoming_orders_data/', 'json',  map("schema", "orderkey INT, customer_key INT, order_status STRING, total_price FLOAT, order_date STRING, priority STRING, clerk STRING, ship_priority INT, comment STRING","multiLine", "true", "lineSep", ","))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Building a live table on a live table
-- MAGIC
-- MAGIC We can then take that same table, and build *another* table on top of it that filters our rows out, which in this case, only contains changes with a last_updated date of today.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE todays_ods_changes
AS
SELECT * FROM STREAM(LIVE.incoming_ods_changes) WHERE last_updated > current_date()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Build our DLT Pipeline
-- MAGIC
-- MAGIC With the code defined in this notebook, we can turn around and build our DLT pipeline in Databricks:
-- MAGIC
-- MAGIC ![image info](img/DLT.png)
