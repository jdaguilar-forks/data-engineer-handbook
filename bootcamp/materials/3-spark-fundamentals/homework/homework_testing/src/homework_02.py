import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncrementalHostActivity").getOrCreate()

# PostgreSQL connection properties
jdbc_url = os.getenv("JDBC_URL")
connection_properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

# Read the events table from PostgreSQL
events_df = spark.read.jdbc(
    url=jdbc_url, table="events", properties=connection_properties
)
events_df.createOrReplaceTempView("events")

# Execute the SQL query
incremental_host_activity_df = spark.sql("""
    SELECT host_id,
           ARRAY_AGG(DISTINCT event_date) AS host_activity_datelist
    FROM events
    WHERE event_date >= DATE_SUB(CURRENT_DATE, 1)
    GROUP BY host_id
""")

incremental_host_activity_df.write.mode("append").saveAsTable("hosts_cumulated")