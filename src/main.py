from datetime import datetime, timedelta
from typing import List

import sqlalchemy as sa

pattern = '%Y-%m-%dT%H:%M:%S'
event_count = [0] * 5
current_position = 0

metadata = sa.MetaData()
detections_table = sa.Table(
    "detections",
    metadata,
    sa.Column('id',   sa.types.Integer, primary_key=True, autoincrement=True),
    sa.Column('time', sa.types.DateTime(timezone=True)),
    sa.Column('type', sa.types.String()),
)

def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
    conn = engine.connect()
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )
    conn.commit()

    return conn


def ingest_data(conn: sa.Connection, data: List[tuple[str, str]], batch_size=500):
    '''
    Bulk insert is better than writing individual rows.
    Best approach would be to write in batches if a lot of data is
    to be written. Helps in error handling. For the sake of brevity
    i am not including error handling
    :param conn: Database connection
    :param data: Data to be written
    :return: None
    '''
    step = 0
    while step * batch_size < len(data):
        insert_values = [{ 'time': row[0], 'type': row[1]} for row in data[step*batch_size: (step+1)*batch_size]]
        conn.execute(sa.insert(detections_table).values(insert_values))
        conn.commit()
        step += 1

def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    '''
    I have used 4 ctes to get results for both people and vehicles category in one go
    We can query for them separately, in which case we can eliminate the first cte where we are only
    categorizing the event and sorting them
    I have not included a time filter for running aggregation. As the problem mentions that there will be
    a lot of data being generated which means that it will be more efficient to query this table on
    a subset of time range instead of the complete table
    Before returning the result, i am also formatting datetime objects into the pattern used during ingestion
    Also, the column type for time includes tz but it is not provided in the examples. The system assumes it to be
    UTC right now, but the format can be updated to include tz
    :param conn:
    :return:
    '''
    sql_statement = '''
    with
        categorized_data as (
            select time as timestamp,
                   case when type in ('pedestrian', 'bicycle') then 'people' else 'vehicle' end as category
            from detections
            order by category, timestamp
        ),
        boundaries as (
            select timestamp, category,
                   LAG(timestamp) over (partition by category ORDER BY timestamp) as prev
            from categorized_data
        ),
        intervals as (
            select
                timestamp, category,
                case when prev is null or timestamp - prev > INTERVAL '1 minute' then 1 else 0 end as interval
            from boundaries
        ),
        final_interval as (
        select
            timestamp, interval, category,
            sum(interval) OVER (ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as number
        from intervals
    )
    select
        min(timestamp) as start,
        max(timestamp) as endTime,
        category
    from final_interval
    group by category, number
    order by category, start
    '''
    results = conn.execute(sa.text(sql_statement)).all()
    response = {'people': [], 'vehicle': []}
    for interval in results:
        response.get(interval[2], []).append(
            (interval[0].strftime(pattern), interval[1].strftime(pattern)))

    return response


def log_alerts_from_aggregation_results(results: List[tuple[str, str]]):
    '''
    Here we are checking the start and end interval of the aggregate results
    to see if we have any interval which is greater than 5 counts of 30 sec i.e. 5 * 30
    However, i find this to be slightly flawed based on the conditions for alert and aggregation
    if a person is detected at interval of 1 min for 3 minutes, should it be alerted? The
    detection interval of 30 sec would mean that it should not be alerted while our assumption
    of 150 sec means it would be alerted.
    We can take advantage of the fact that we have to only check for last 150 seconds after ingestion
    of data and see if person has been detected 5 times in it or not
    I will write a separate function for doing that
    :param results: Aggregation results of query run earlier
    :return: None
    '''
    for interval in results:
        start: datetime = datetime.strptime(interval[0], pattern)
        end: datetime = datetime.strptime(interval[1], pattern)
        if (end - start).seconds > 30 * 5:
            print('Person detected')


def check_for_alerts(conn: sa.Connection, interval=150, hit_threshold=5):
    '''
    We can use current time floored to nearest 30 sec interval and use that to check
    the no of times a person has been detected
    :param conn: database connection
    :param interval: time interval to consider for alert check
    :param hit_threshold: max allowed occurrences of a person
    :return: None
    '''
    current_time: datetime = datetime.now().replace(microsecond=0)
    if current_time.second > 30:
        current_time = current_time.replace(second=30)
    else:
        current_time = current_time.replace(second=0)
    start_time = (current_time - timedelta(seconds=interval))
    timestamp_query_pattern = '%Y-%m-%d %H:%M:%S'
    sql_statement = '''
    select count(*)
    from detections
    where type in ('pedestrian', 'bicycle')
    and time > :start_time
    '''
    result = conn.execute(
        sa.text(sql_statement).bindparams(start_time=start_time.strftime(timestamp_query_pattern))).all()
    if result[0][0] > hit_threshold:
        print('Person found for %s times in last %s seconds' % (hit_threshold, interval))


def trigger_alert(current_position: int, event_count: List[int], persons_count: int, hit_threshold = 5):
    '''
    This uses a in memory solution where you keep count of last 5 intervals
    for each interval in a list with a pointer to current value to be written
    For each new set of data, we can update the latest pointer and take a sum of the
    list to find out if we need to trigger an alert
    :return: None
    '''
    event_count[current_position] = persons_count
    current_position = (current_position + 1) % 5
    if sum(event_count) > hit_threshold:
        print('Person found %s times in last 150 seconds' % sum(event_count))




def main():
    conn = database_connection()

    # Simulate real-time detections every 30 seconds
    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    ingest_data(conn, detections, 100)

    aggregate_results = aggregate_detections(conn)
    print(aggregate_results)
    # I have 3 methods to log alerts
    log_alerts_from_aggregation_results(aggregate_results['people'])
    check_for_alerts(conn, 150, 5)
    person_count = sum(1 for event in detections if event[1] in ('pedestrian', 'bicycle'))
    trigger_alert(current_position, event_count, person_count, 5)


if __name__ == "__main__":
    main()
