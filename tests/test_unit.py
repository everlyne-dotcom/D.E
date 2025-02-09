import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *

from jobs.unit_job import do_team_vertex_transformation, do_events_vertex_transformation
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

EventsVertex = namedtuple("EventsVertex", "identifier type properties")
Event = namedtuple("Event", "user_id event_time")

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("unit-tests") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_vertex_generation(spark):
    input_data = [
        Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_team_vertex_transformation(spark, input_dataframe)
    expected_output = [
        TeamVertex(
            identifier=1,
            type='team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

    #events:Event = namedtuple("Event", "user_id event_time")
    def test_events_generation(spark):
        inserted_data = [
            Event(user_id=1, event_time="2023-03-30"),
            Event(user_id=1, event_time="2023-03-31"),
        ]
        input_dataframe = spark.createDataFrame(inserted_data)
        actual_df = do_events_vertex_transformation(spark, input_dataframe)
        outlined_output = [
            EventsVertex(
                identifier=1,
                type='event',
                properties={
                    'user_id': 1,
                    'event_time': '2023-03-30'
                }
            )
        ]
        expected_df = spark.createDataFrame(outlined_output)
        assert_df_equality(actual_df, expected_df, ignore_nullable=True)