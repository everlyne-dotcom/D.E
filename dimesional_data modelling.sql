--query1
CREATE TYPE films AS (
                  film TEXT,
				  votes INT,
				  rating REAL, 
				  filmid TEXT				  
)

--array of struct, and table
CREATE TABLE actor(
        actor  TEXT,
		actorid TEXT PRIMARY KEY,
		film films[],
		year INT,
		quality_class VARCHAR(20),
        is_active BOOLEAN
)  


WITH quality AS (
    SELECT
        a.actorid,
        a.actor,
        a.film,
          CASE
              WHEN AVG(f.rating) > 8 THEN 'star'
              WHEN AVG(f.rating) > 7 THEN 'good'
              WHEN AVG(f.rating) > 6 THEN 'average'
              ELSE 'bad'
          END AS quality_class
    FROM actor a
	CROSS JOIN UNNEST(a.film) AS f
	GROUP BY a.actorid, a.actor, a.film, a.quality_class
)
UPDATE actor a
SET quality_class = q.quality_class
FROM quality q
WHERE a.actorid = q.actorid;


UPDATE actor
SET is_active = TRUE
WHERE EXISTS (
    SELECT 1
    FROM unnest(actor.film) AS f(film, votes, rating, filmid)
    WHERE EXTRACT(YEAR FROM CURRENT_DATE) = year
	)

--query2:
INSERT INTO actor(actor, actorid, film, year)
SELECT  
    af.actor,										
    af.actorid,
	
    ARRAY(
        SELECT DISTINCT ROW(af.film, af.votes, af.rating, af.filmid)::films
        FROM actor_films sub_af
        WHERE sub_af.actorid = af.actorid
    ) AS film,
	af.year
FROM  actor_films af
ON CONFLICT (actorid) DO NOTHING;


--query3:
CREATE TABLE actors_history_scd (
    actorid TEXT PRIMARY KEY,
    actor TEXT,
    film films[],
    year INT,
    quality_class VARCHAR(20),
    is_active BOOLEAN,
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    end_date DATE DEFAULT NULL, -- NULL indicates the current version
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

--query4:
backfilled AS (
    SELECT
        q.actorid,
        q.actor,
        q.films,
        q.year,
        q.quality_class,
        q.is_active,
        q.start_date,
        NULL::DATE AS end_date,  -- Set the initial end_date as NULL for the backfill
        TRUE AS is_current
    FROM quality q
)
-- Insert the backfilled data into the actor_history table
INSERT INTO actor_history_scd (actorid, actor, films, year, quality_class, is_active, start_date, end_date, is_current)
SELECT
    bf.actorid,
    bf.actor,
    bf.films,
    bf.year,
    bf.quality_class,
    bf.is_active,
    bf.start_date,
    bf.end_date,
    bf.is_current
FROM backfilled bf
ON CONFLICT (actorid, year) DO NOTHING;

--query5:
WITH new_data AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        CURRENT_DATE AS start_date
    FROM actor
),
updates AS (
    UPDATE actors_history_scd scd
    SET end_date = CURRENT_DATE - INTERVAL '1 day',  -- Set end date to yesterday
        is_current = FALSE
    FROM new_data nd
    WHERE scd.actorid = nd.actorid
    AND scd.end_date IS NULL  -- Only close current records
    AND (scd.quality_class != nd.quality_class OR 
         scd.is_active != nd.is_active)  -- Only for changes
    RETURNING scd.actorid, scd.actor, scd.quality_class, scd.is_active, 
              scd.start_date, CURRENT_DATE - INTERVAL '1 day' AS end_date
    ),
inserts AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        NULL::DATE AS end_date
    FROM new_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM actors_history_scd scd
        WHERE scd.actorid = nd.actorid
        AND scd.end_date IS NULL
)
)
INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date)
SELECT actorid, actor, quality_class, is_active, start_date, end_date FROM updates
UNION ALL
SELECT actorid, actor, quality_class, is_active, start_date, end_date FROM inserts;