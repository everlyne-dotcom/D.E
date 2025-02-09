import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, broadcast

#initialize spark session
spark = SparkSession.builder \
         .appName("Game Analytics") \
         .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
         .getOrCreate()
#load the data 
raw_url = "https://raw.githubusercontent.com/DataExpert-io/data-engineer-handbook/refs/heads/main/bootcamp/materials/3-spark-fundamentals/data/maps.csv"
response = requests.get(raw_url)
with open("maps.csv", "wb") as file:
    file.write(response.content)
    
git_url="https://raw.githubusercontent.com/DataExpert-io/data-engineer-handbook/refs/heads/main/bootcamp/materials/3-spark-fundamentals/data/match_details.csv"
response = requests.get(git_url)
with open("match_details.csv", "wb") as file:
     file.write(response.content)

hub_url="https://raw.githubusercontent.com/DataExpert-io/data-engineer-handbook/refs/heads/main/bootcamp/materials/3-spark-fundamentals/data/matches.csv"
response = requests.get(hub_url)
with open("matches.csv", "wb") as file:
    file.write(response.content)

raws_url="https://raw.githubusercontent.com/DataExpert-io/data-engineer-handbook/refs/heads/main/bootcamp/materials/3-spark-fundamentals/data/medals.csv"
response = requests.get(raws_url)
with open("medals.csv", "wb") as file:
    file.write(response.content)

rawz_url="https://raw.githubusercontent.com/DataExpert-io/data-engineer-handbook/refs/heads/main/bootcamp/materials/3-spark-fundamentals/data/medals_matches_players.csv"
response = requests.get(rawz_url)
with open("medal_matches_players.csv", "wb") as file:
    file.write(response.content)
    

#load the datasets
maps = spark.read.csv("maps.csv", header=True, inferSchema=True)
matchDetailsBucketed = spark.read.csv("match_details.csv", header=True, inferSchema=True)
matchesBucketed = spark.read.csv("matches.csv", header=True, inferSchema=True)
medals = spark.read.csv("medals.csv", header=True, inferSchema=True)
medalMatchesPlayersBucketed = spark.read.csv("medal_matches_players.csv", header=True, inferSchema=True)

# Broadcast join: medals and maps
medals_broadcast = broadcast(medals)
maps_broadcast = broadcast(maps)
matches_maps = matchesBucketed.join(maps_broadcast, "mapid")
matches_maps_details = matches_maps.join(medalMatchesPlayersBucketed, "match_id")
matches_maps_medals = matches_maps_details.crossJoin(medals_broadcast)
#bucketjoin
#match_details_bucket table
# Creating the table (this will be done in the Spark SQL engine)
spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.bucketed_match_details (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
""")

match_details_bucket = matchDetailsBucketed \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.bucketed_match_details")

spark.sql( """
CREATE TABLE IF NOT EXISTS bootcamp.bucketed_matches (
     match_id STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (completion_date, bucket(16, match_id));
""")
match_bucket = matchesBucketed \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bootcamp.bucketed_matches")

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.bucketed_medals_matches_players(
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING
)
""")
medal_matches_players_bucket = medalMatchesPlayersBucketed \
    .write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .saveAsTable("bucketed_medals_matches_players")



spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

matchesBucketed.createOrReplaceTempView("matches")
matchDetailsBucketed .createOrReplaceTempView("match_details")
medalMatchesPlayersBucketed.createOrReplaceTempView("medal_matches_players")


spark.sql("""
SELECT 
          mdb.match_id,
          mdb.player_gamertag,
          m.completion_date
 FROM bootcamp.buckted_match_details mdb 
JOIN bootcamp.bucketed_matches m
ON mdb.match_id = m.match_id
AND m.completion_date = DATE('2016-01-01')
""").explain()

spark.sql("""
   SELECT *  FROM match_details mdb JOIN matches m ON mdb.match_id = m.match_id
""").explain()

spark.sql("""
SELECT 
          m.match_id,
          mmp.player_gamertag,
          m.completion_date,
          m.playlist_id
FROM bootcamp.bucketed_matches m 
JOIN bootcamp.bucketed_medals_matches_players mmp
   ON m.match_id = mmp.match_id
   AND m.completion_date = DATE('2016-01-01')
   """).explain()

spark.sql("""
SELECT * FROM matches m JOIN medal_matches_players mmp ON m.match.id = mmp.match_id
""").explain()

#aggregate the data
# players with the highest average kills per game
player_avg_kills = spark.sql("""
SELECT
   player_gamertag,
   AVG(player_total_kills) AS avg_kills
FROM
    bootcamp.bucketed_match_details
GROUP BY 
     player_gamertag
ORDER BY 
     avg_kills DESC
LIMIT 10
""")
print("Top players by average kills:")
player_avg_kills.show()
#playlist that gets played the most
most_played_playlist = spark.sql("""
SELECT 
    playlist_id,
    COUNT(*) AS play_count
FROM 
    bootcamp.bucketed_matches
GROUP BY 
    playlist_id
ORDER BY 
    play_count DESC
LIMIT 10
""")
print("Most played playlist:")
most_played_playlist.show()
#most played on the map
most_played_map = spark.sql("""
SELECT 
   mapid AS map_id,
   COUNT(*) AS play_count
FROM 
    matches
GROUP BY
    mapid
ORDER BY 
  play_count DESC
LIMIT 10
""")
print("Most played on map:")
most_played_map.show()
#killing spree count
killing_spree_map_aggregated = matches_maps_medals.groupBy("mapid") \
    .agg(count("classification").alias("killing_spree_count")) \
    .orderBy(desc("killing_spree_count"))
print("Maps with the most 'Killing Spree' medals:")
killing_spree_map_aggregated.show()

# Sorting within partitions for playlists
most_played_playlist.write.mode("overwrite") \
                     .sortWithinPartitions("playlist_id") \
                    .parquet("sorted_by_playlist")

# Sorting within partitions for maps
most_played_map.write.mode("overwrite") \
                   .sortWithinPartitions("map_id")\
                   .parquet("sorted_by_map")

# Partition by completion_date and match_id
matchesBucketed.write.mode("overwrite") \
    .sortWithinPartitions("completion_date", "match_id") \
    .parquet("partitioned_matches_date_id")

# Partition by player_gamertag
matchDetailsBucketed.write.mode("overwrite") \
    .sortWithinPartitions("player_gamertag") \
    .parquet("partitioned_match_details_gamertag")

