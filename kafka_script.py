from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table import StreamTableEnvironment
from datetime import datetime, timedelta

class WebEvent:
    def __init__(self, ip, host, timestamp):
        self.ip = ip
        self.host = host
        self.timestamp = timestamp

def web_event_mapper(event):
    return (event.ip + "_" + event.host, 1)

def sum_events(e1, e2):
    return e1 + e2

def avg_session_size(result):
    host = result[0].split("_")[1]
    return (host, result[1])

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

events = env.from_collection([
    WebEvent("192.168.1.1", "zachwilson.techcreator.io", datetime.now().timestamp()),
    WebEvent("192.168.1.1", "zachwilson.techcreator.io", (datetime.now() + timedelta(seconds=10)).timestamp()),
    WebEvent("192.168.1.2", "zachwilson.tech", (datetime.now() + timedelta(minutes=5)).timestamp()),
    WebEvent("192.168.1.3", "lulu.techcreator.io", (datetime.now() + timedelta(minutes=6)).timestamp())
])

events = events.assign_timestamps_and_watermarks(WatermarkStrategy.for_bounded_out_of_orderness(Time.seconds(10)))

sessionized = events.map(web_event_mapper) \
    .key_by(lambda e: e[0]) \
    .window(EventTimeSessionWindows.with_gap(Time.minutes(5))) \
    .reduce(sum_events)

average_sessions = sessionized.map(avg_session_size) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (a[0], (a[1] + b[1]) / 2))

# Define SQL Table for storing sessionized data in PostgreSQL
t_env.execute_sql("""
    CREATE TABLE session_results (
        host STRING,
        avg_events_per_session DOUBLE
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/flink_db',
        'table-name' = 'session_results',
        'driver' = 'org.postgresql.Driver',
        'username' = 'flink_user',
        'password' = 'flink_password'
    )
""")

table = t_env.from_data_stream(average_sessions, ['host', 'avg_events_per_session'])
table.execute_insert("session_results")

env.execute("Web Sessionization Job")
