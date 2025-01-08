from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, when, count, avg


if __name__ == "__main__":

    # Initialize Spark session
    spark = SparkSession.builder.appName("AggregatePlayerStats").getOrCreate()

    # Disable automatic broadcast join to control join strategy manually
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Read CSV files into DataFrames
    df_match_details = spark.read.csv(
        "../../data/match_details.csv",
        header=True,
        inferSchema=True,
    )
    df_matches = spark.read.csv(
        "../../data/matches.csv",
        header=True,
        inferSchema=True,
    )
    df_medals_matches_players = spark.read.csv(
        "../../data/medals_matches_players.csv",
        header=True,
        inferSchema=True,
    )
    df_medals = spark.read.csv(
        "../../data/medals.csv",
        header=True,
        inferSchema=True,
    )
    df_maps = spark.read.csv(
        "../../data/maps.csv",
        header=True,
        inferSchema=True,
    )

    # Broadcast join medals and maps for efficient joining
    df_medals_broadcast = broadcast(df_medals)
    df_maps_broadcast = broadcast(df_maps)

    # Bucket join match_details, matches, and medals_matches_players on match_id with 16 buckets
    df_match_details.write.bucketBy(16, "match_id").saveAsTable(
        "warehouse.bucketed_match_details", mode="overwrite"
    )
    df_matches.write.bucketBy(16, "match_id").saveAsTable(
        "warehouse.bucketed_matches", mode="overwrite"
    )
    df_medals_matches_players.write.bucketBy(16, "match_id").saveAsTable(
        "warehouse.bucketed_medals_matches_players", mode="overwrite"
    )

    # Read the bucketed tables
    match_details_bucketed = spark.table("warehouse.bucketed_match_details")
    matches_bucketed = spark.table("warehouse.bucketed_matches")
    medals_matches_players_bucketed = spark.table(
        "warehouse.bucketed_medals_matches_players"
    )

    # Perform joins with aliases for better readability
    joined_df = (
        match_details_bucketed.alias("md")
        .join(matches_bucketed.alias("m"), on="match_id")
        .join(medals_matches_players_bucketed.alias("mp"), on="match_id")
        .join(df_medals_broadcast.alias("med"), on="medal_id")
        .join(df_maps_broadcast.alias("map"), on="mapid")
    )

    # Aggregate data with column aliases for better readability
    aggregated_df = joined_df.groupBy("md.player_gamertag").agg(
        avg("md.player_total_kills").alias("avg_kills_per_game"),
        count("m.playlist_id").alias("playlist_count"),
        count("map.name").alias("map_count"),
        count(when(col("med.name") == "Killing Spree", True)).alias(
            "killing_spree_count"
        ),
    )

    # Find the player who averages the most kills per game
    most_kills_player = (
        aggregated_df.orderBy(col("avg_kills_per_game").desc())
        .select("player_gamertag", "avg_kills_per_game")
        .first()
    )
    print(
        f"Player with the most average kills per game: {most_kills_player['player_gamertag']} with {most_kills_player['avg_kills_per_game']} kills per game"
    )

    # Find the most played playlist
    most_played_playlist = (
        joined_df.groupBy("playlist_id")
        .count()
        .orderBy(col("count").desc())
        .select("playlist_id", "count")
        .first()
    )
    print(
        f"Most played playlist: {most_played_playlist['playlist_id']} with {most_played_playlist['count']} plays"
    )

    # Find the most played map
    most_played_map = (
        joined_df.groupBy("map.name")
        .count()
        .orderBy(col("count").desc())
        .select("name", "count")
        .first()
    )
    print(
        f"Most played map: {most_played_map['name']} with {most_played_map['count']} plays"
    )

    # Find the map where players get the most Killing Spree medals
    most_killing_spree_map = (
        joined_df.filter(col("med.name") == "Killing Spree")
        .groupBy("map.name")
        .count()
        .orderBy(col("count").desc())
        .select("name", "count")
        .first()
    )
    print(
        f"Map with the most Killing Spree medals: {most_killing_spree_map['name']} with {most_killing_spree_map['count']} medals"
    )

    # Sort within partitions by map_count
    sorted_by_map = aggregated_df.sortWithinPartitions("map_count")

    # Sort within partitions by player_gamertag
    sorted_by_player = aggregated_df.sortWithinPartitions("player_gamertag")

    # Sort within partitions by playlist_count
    sorted_by_playlist = aggregated_df.sortWithinPartitions("playlist_count")

    # Save sorted DataFrames as tables
    sorted_by_player.write.partitionBy("player_gamertag").saveAsTable(
        "warehouse.sorted_by_player_table", mode="overwrite"
    )
    sorted_by_map.write.partitionBy("map_count").saveAsTable("warehouse.sorted_by_map_table", mode="overwrite")
    sorted_by_playlist.write.partitionBy("playlist_count").saveAsTable(
        "warehouse.sorted_by_playlist_table", mode="overwrite"
    )

    # Print size for sorted_by_map
    size_sorted_by_map = spark.sql(
        """
        SELECT SUM(file_size_in_bytes) as size FROM warehouse.sorted_by_map_table.files
    """
    ).first()["size"]
    print(f"Size of sorted_by_map_table: {size_sorted_by_map}")

    # Print size for sorted_by_playlist
    size_sorted_by_playlist = spark.sql(
        """
        SELECT SUM(file_size_in_bytes) as size FROM warehouse.sorted_by_playlist_table.files
    """
    ).first()["size"]
    print(f"Size of sorted_by_playlist_table: {size_sorted_by_playlist}")

    # Print size for sorted_by_player
    size_sorted_by_player = spark.sql(
        """
        SELECT SUM(file_size_in_bytes) as size FROM warehouse.sorted_by_player_table.files
    """
    ).first()["size"]
    print(f"Size of sorted_by_player_table: {size_sorted_by_player}")
