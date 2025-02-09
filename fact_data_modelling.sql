--query1: dedudplicate the data
WITH deduped AS (
   SELECT 
   *, ROW_NUMBER() OVER (PARTITION BY  game_id, team_id, player_id) AS row_num
   FROM game_details
)
SELECT * FROM deduped 
WHERE row_num = 1

--query2: a ddl for the table
CREATE TABLE user_devices_cumulated (
    user_id INT ,
    browser_type TEXT,
    active_days DATE[]
);

--query3: a cumultive query
SELECT
     user_id,
    d.browser_type,
    ARRAY_AGG(event_time) AS active_days
FROM events e
JOIN devices d ON e.device_id = d.device_id
GROUP BY user_id, d.browser_type;


--query4: datelist_int
SELECT
    user_id,
    browser_type,
    ARRAY_AGG(EXTRACT(EPOCH FROM d)::BIGINT) AS datelist_int
FROM (
    SELECT
        user_id,
        browser_type,
        UNNEST(active_days) AS d
    FROM user_devices_cumulated
) subquery
GROUP BY user_id, browser_type;


--query5: a ddl for the table
CREATE TABLE hosts_cumulated (
    host INT,
    host_activity_datelist DATE[]
);


--query6: a incremental query
SELECT
    host,
    ARRAY_AGG(event_time) AS host_activity_datelist
FROM events
GROUP BY host;


--query7: ddl
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array INTEGER[],            
    unique_visitors INTEGER[]       
);
--query8: a incremental query
WITH daily_counts AS (
    SELECT
        DATE(e.event_time::timestamp) AS month,  
        e.host,
        COUNT(1) AS hit_count,
        COUNT(DISTINCT e.user_id) AS unique_visitors_count
    FROM events e
    WHERE e.event_time >= '2023-01-01'  
    GROUP BY e.host, DATE( e.event_time::timestamp), e.event_time
)
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors)
SELECT
    month,
    host,
    ARRAY_AGG(hit_count) AS hit_array,  
    ARRAY_AGG(unique_visitors_count) AS unique_visitors  
FROM daily_counts
GROUP BY month, host;



