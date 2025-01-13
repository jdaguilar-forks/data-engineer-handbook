from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("CumulativeDeviceActivity").getOrCreate()

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
device_activity_datelist_df = spark.sql("""
    SELECT user_id,
           browser_type,
           ARRAY_AGG(DISTINCT event_date) AS device_activity_datelist
    FROM events
    GROUP BY user_id, browser_type
""")

device_activity_datelist_df.write.mode("overwrite").saveAsTable(
    "cumulative_device_activity_datelist"
)