# databricks-ods-redis-examples

This repository shows an example of how you can build a process to ingest data (via DLT and Workflows) to process changes in a table in your Lakehouse, and then use the power of spark-redis to persist changes in a Redis cache.

## Notebook descriptions:

* **Refresh Redis Cache.py** - This notebook contains example code of how, via PySpark, you can read data from a source (like a Delta table) into a Dataframe, and then process updates to a another table, and then turn around and write the data into an Azure Redis Cache
* **DLT SQL Incoming Changes.sql** - This notebook is designed to show how you can read JSON files off of a cloud storage account, and write the incoming cata (whether they be changes or new records) to a Delta Live Table (which is used in the next notebook)
* **Update ODS And Redis From Changes.sql** - This notebook, written in pure SQL, shows how using the changes ingested via the Delta Live Table process, the changes are persisted back to the table using a MERGE statement. The same changes are then sent to the Redis cache.

## Running these examples

These notebooks are designed to show how changes can processed from a Delta Table/Unity Catalog/Hive Metastore source in a Databricks workspace. You can adjust the source tables to meet any data sources you want to try this with. You should also make sure you have the following set up:

1. An Azure Redis Cache: You can use any Redis destination you want, but this demo was built on the Azure-managed Redis offering. You can learn more about the offering here: https://azure.microsoft.com/en-us/products/cache. Make sure you deploy or scale your deployment to meet your cache/size requirements.
2. You will need a cluster to run the examples. Your cluster should be able to communicate with the Redis destination, which you can set via the Spark configuration of the cluster: 

        
        spark.redis.port <your redis port number>
        spark.redis.host <your redis hostname>
        spark.redis.auth <your redis key>
        
        
  * Note: By default, Azure Redis requires SSL and operates on a different default port; for exploration and testing, you may want to enable non-SSL to get up and running until you can properly configure SSL on your cluster.

3. You will also need to make sure you install the spark-redis package on your cluster. You can do this via cluster libaries and using the Maven coordinate for the package: https://mvnrepository.com/artifact/com.redislabs/spark-redis_2.12/3.1.0

### Links

* spark-redis package: [https://github.com/RedisLabs/spark-redis](https://github.com/RedisLabs/spark-redis)
* Databricks DLT: [https://www.databricks.com/discover/pages/getting-started-with-delta-live-tables](https://www.databricks.com/discover/pages/getting-started-with-delta-live-tables)
* Databricks Workflows: [https://docs.databricks.com/en/workflows/index.html](https://docs.databricks.com/en/workflows/index.html)