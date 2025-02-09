SELECT 
    p.player_name,
	start_season,
	end_season,
    CASE 
       WHEN ps.start_season  IS NOT NULL AND ps.end_season IS NULL THEN 'New'
       WHEN ps.start_season IS NULL AND ps.end_season IS NOT NULL THEN 'Retired'
       WHEN ps.start_season IS NOT NULL AND ps.end_season IS NOT NULL THEN 'Returned from Retirement'
       WHEN ps.start_season IS NULL AND ps.end_season IS NULL THEN 'Stayed Retired'
       ELSE 'Continued Playing'
    END AS player_status
FROM 
    player p
LEFT JOIN 
    players_scd ps ON p.player_name = ps.player_name;


--query 2: 
--player and team
SELECT 
    player_id, 
    team_id,  
    SUM(pts) AS total_points
FROM 
    game_details
GROUP BY GROUPING SETS (
 (player_id),
 (team_id),
 (player_id, team_id) )
ORDER BY total_points DESC;

--player and seasons
SELECT
Max(MIN)
FROM game_details

SELECT * FROM game_details;
SELECT 
   player_name,
   player_id,
   SUM(pts) AS total_points
FROM game_details
GROUP BY GROUPING SETS(
     (player_name),
	 (player_id),
	 (player_name, player_id)
)
ORDER BY total_points DESC

--team
SELECT 
  team_id,
  SUM(pts) as total_points
 FROM game_details
  GROUP BY GROUPING SETS (
  (team_id),
  (team_id)
   )
  ORDER BY total_points 

-- query 3

  SELECT
	SUM(
        CASE WHEN 
            CAST(split_part(min, ':', 1) AS INTEGER) + 
            CAST(split_part(min, ':', 2) AS INTEGER) / 60.0 > 90 
        THEN 1 ELSE 0 END
    )
	  OVER (
        PARTITION BY team_id 
        ORDER BY game_details.min ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS high_performance_games_in_90,
	team_id,
    game_id
FROM 
    game_details
WHERE 
    min LIKE '[0-9][0-9]:[0-9][0-9]' 
GROUP BY 
    team_id, game_id, game_details.min
ORDER BY 
    team_id, game_id;


--last query 
SELECT 
    player_id,
    game_id,
    pts,
    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_id) -
    ROW_NUMBER() OVER (PARTITION BY player_id, (CASE WHEN pts > 10 THEN 1 ELSE 0 END) ORDER BY game_id) AS streak_id
FROM 
    game_details
WHERE 
    player_id = 2544 AND pts > 10
ORDER BY game_id;
select * from game_details
where team_city = 'Los Angeles' and player_name = 'LeBron James'
 

