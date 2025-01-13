import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, current_date, date_sub

class IncrementalHostActivityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestIncrementalHostActivity").getOrCreate()
        cls.events_data = [
            ("host1", "2023-10-01"),
            ("host1", "2023-10-02"),
            ("host2", "2023-10-01"),
            ("host3", "2023-10-03")
        ]
        cls.events_df = cls.spark.createDataFrame(cls.events_data, ["host_id", "event_date"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def assertDataFrameEqual(self, df1, df2):
        self.assertEqual(sorted(df1.collect()), sorted(df2.collect()))

    def test_incremental_host_activity(self):
        result_df = self.events_df.filter(self.events_df.event_date >= date_sub(current_date(), 1)) \
            .groupBy("host_id") \
            .agg(collect_set("event_date").alias("host_activity_datelist"))

        expected_data = [
            ("host1", ["2023-10-02"]),
            ("host2", ["2023-10-01"]),
            ("host3", ["2023-10-03"])
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["host_id", "host_activity_datelist"])

        self.assertDataFrameEqual(result_df, expected_df)

    def test_no_recent_activity(self):
        result_df = self.events_df.filter(self.events_df.event_date >= date_sub(current_date(), 0)) \
            .groupBy("host_id") \
            .agg(collect_set("event_date").alias("host_activity_datelist"))

        expected_data = []
        expected_df = self.spark.createDataFrame(expected_data, ["host_id", "host_activity_datelist"])

        self.assertDataFrameEqual(result_df, expected_df)

    def test_all_hosts_activity(self):
        result_df = self.events_df.groupBy("host_id") \
            .agg(collect_set("event_date").alias("host_activity_datelist"))

        expected_data = [
            ("host1", ["2023-10-01", "2023-10-02"]),
            ("host2", ["2023-10-01"]),
            ("host3", ["2023-10-03"])
        ]
        expected_df = self.spark.createDataFrame(expected_data, ["host_id", "host_activity_datelist"])

        self.assertDataFrameEqual(result_df, expected_df)

if __name__ == "__main__":
    unittest.main()