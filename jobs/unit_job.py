from pyspark.sql import SparkSession

query= """
WITH teams_deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY team_id) AS row_num
    FROM teams
)
 SELECT 
    team_id AS identifier,
    'team' AS 'type',
    struct(
    'abbreviation', abbreviation,
    'nickname', nickname,
    'city', city,
    'arena', arena,
    'year_founded', year_founded
    ) AS properties
FROM teams_deduped
WHERE row_num = 1

"""

def do_team_vertex_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)

def main ():
    spark = SparkSession.builder \
        .master("local") \
        .appName("player_scd") \
        .getOrCreate()
    output_df = do_team_vertex_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("player_scd")

query = """
WITH yesterday AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2023-03-30')
),
    today AS (
          SELECT user_id,
                 DATE_TRUNC('day', event_time) AS today_date,
                 COUNT(1) AS num_events FROM events
            WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-31')
            AND user_id IS NOT NULL
         GROUP BY user_id,  DATE_TRUNC('day', event_time)
    ) AS properties
INSERT INTO users_cumulated
SELECT
       COALESCE(t.user_id, y.user_id),
       COALESCE(y.dates_active,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.user_id IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS date_list,
       COALESCE(t.today_date, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id;
"""

def do_events_vertex_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)

def main ():
    spark = SparkSession.builder \
        .master("local") \
        .appName("events_timeline") \
        .getOrCreate()
    output_df = do_events_vertex_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("events_timeline")