import unittest
import os
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set


class CumulativeDeviceActivityTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "TestCumulativeDeviceActivity"
        ).getOrCreate()
        self.events_data = [
            ("user1", "chrome", "2023-10-01"),
            ("user1", "chrome", "2023-10-02"),
            ("user2", "firefox", "2023-10-01"),
        ]
        self.events_df = self.spark.createDataFrame(
            self.events_data, ["user_id", "browser_type", "event_date"]
        )

    def test_device_activity_datelist(self):
        result_df = self.events_df.groupBy("user_id", "browser_type").agg(
            collect_set("event_date").alias("device_activity_datelist")
        )

        expected_data = [
            ("user1", "chrome", ["2023-10-01", "2023-10-02"]),
            ("user2", "firefox", ["2023-10-01"]),
        ]
        expected_df = self.spark.createDataFrame(
            expected_data, ["user_id", "browser_type", "device_activity_datelist"]
        )

        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_read_events_table(self):
        # Mock the environment variables
        os.environ["JDBC_URL"] = "jdbc:postgresql://localhost:5432/testdb"
        os.environ["DB_USER"] = "testuser"
        os.environ["DB_PASSWORD"] = "testpassword"

        # Mock the SparkSession read.jdbc method
        with patch.object(self.spark.read, "jdbc") as mock_read_jdbc:
            mock_read_jdbc.return_value = self.events_df

            # Call the function to read the events table
            events_df = self.spark.read.jdbc(
                url=os.getenv("JDBC_URL"),
                table="events",
                properties={
                    "user": os.getenv("DB_USER"),
                    "password": os.getenv("DB_PASSWORD"),
                    "driver": "org.postgresql.Driver",
                },
            )

            # Verify that the read.jdbc method was called with the correct arguments
            mock_read_jdbc.assert_called_once_with(
                url="jdbc:postgresql://localhost:5432/testdb",
                table="events",
                properties={
                    "user": "testuser",
                    "password": "testpassword",
                    "driver": "org.postgresql.Driver",
                },
            )

            # Verify the DataFrame content
            self.assertEqual(events_df.collect(), self.events_df.collect())

    def test_write_device_activity_datelist(self):
        result_df = self.events_df.groupBy("user_id", "browser_type").agg(
            collect_set("event_date").alias("device_activity_datelist")
        )

        # Mock the DataFrame write method
        with patch.object(result_df, "write") as mock_write:
            mock_write.mode.return_value.saveAsTable = unittest.mock.Mock()

            # Call the function to write the DataFrame
            result_df.write.mode("overwrite").saveAsTable(
                "cumulative_device_activity_datelist"
            )

            # Verify that the write method was called with the correct arguments
            mock_write.mode.assert_called_once_with("overwrite")
            mock_write.mode.return_value.saveAsTable.assert_called_once_with(
                "cumulative_device_activity_datelist"
            )

            # Verify the DataFrame content
            expected_data = [
                ("user1", "chrome", ["2023-10-01", "2023-10-02"]),
                ("user2", "firefox", ["2023-10-01"]),
            ]
            expected_df = self.spark.createDataFrame(
                expected_data,
                ["user_id", "browser_type", "device_activity_datelist"],
            )
            self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main()
