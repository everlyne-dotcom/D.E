-- Query 1: Player State Changes
SELECT p.player_name, ps.start_season, ps.end_season,
    CASE 
       WHEN ps.start_season IS NOT NULL AND ps.end_season IS NULL THEN 'New'
       WHEN ps.start_season IS NULL AND ps.end_season IS NOT NULL THEN 'Stayed Retired'
       WHEN ps.start_season IS NOT NULL AND ps.end_season IS NOT NULL 
            AND ps.start_season > ps.end_season THEN 'Returned from Retirement'
       WHEN ps.start_season IS NOT NULL AND ps.end_season IS NOT NULL THEN 'Retired'
       ELSE 'Continued Playing'
    END AS player_status
FROM player p
LEFT JOIN players_scd ps ON p.player_name = ps.player_name;

-- Query 2: Aggregations with GROUPING SETS
SELECT player_id, team_id, SUM(pts) AS total_points
FROM game_details
GROUP BY GROUPING SETS ((player_id), (team_id), (player_id, team_id))
ORDER BY total_points DESC;

SELECT player_id, season, SUM(pts) AS total_points
FROM game_details
GROUP BY GROUPING SETS ((player_id), (season), (player_id, season))
ORDER BY total_points DESC;

SELECT team_id, SUM(pts) AS total_points
FROM game_details
GROUP BY team_id
ORDER BY total_points DESC;

-- Query 3: Highest Scorer for a Single Team
SELECT player_id, team_id, SUM(pts) AS total_points
FROM game_details
GROUP BY player_id, team_id
ORDER BY total_points DESC
LIMIT 1;

-- Query 4: Highest Scorer in a Single Season
SELECT player_id, season, SUM(pts) AS total_points
FROM game_details
GROUP BY player_id, season
ORDER BY total_points DESC
LIMIT 1;

-- Query 5: Team with Most Wins
SELECT team_id, COUNT(*) AS total_wins
FROM game_details
WHERE win_loss = 'W'
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;

-- Query 6: Most Wins in a 90-Game Stretch
SELECT team_id, game_id, 
    SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END) 
        OVER (PARTITION BY team_id ORDER BY game_id ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_90_games
FROM game_details
ORDER BY wins_in_90_games DESC
LIMIT 1;

-- Query 7: Streak of 10+ Point Games by LeBron James
WITH Streaks AS (
    SELECT player_id, game_id, pts,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_id) AS game_rank,
        ROW_NUMBER() OVER (PARTITION BY player_id, (CASE WHEN pts > 10 THEN 1 ELSE 0 END) ORDER BY game_id) AS streak_rank
    FROM game_details
    WHERE player_id = 2544 AND pts > 10
)
SELECT player_id, COUNT(*) AS streak_length
FROM Streaks
GROUP BY (game_rank - streak_rank)
ORDER BY streak_length DESC
LIMIT 1;
