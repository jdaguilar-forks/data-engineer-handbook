SELECT
    player_id,
    team_id,
    season,
    SUM(points) AS total_points,
    SUM(CASE WHEN result = 'Win' THEN 1 ELSE 0 END) AS total_wins,
    GROUPING(player_id) AS grp_player,
    GROUPING(team_id) AS grp_team,
    GROUPING(season) AS grp_season
FROM game_details
GROUP BY GROUPING SETS (
    (player_id, team_id),
    (player_id, season),
    (team_id)
)
ORDER BY grp_player, grp_team, grp_season;


-- Identify the player who scored the most points for a single team
SELECT
    player_id,
    team_id,
    SUM(points) AS total_points
FROM game_details
GROUP BY player_id, team_id
ORDER BY total_points DESC
LIMIT 1;


-- Identify the player who scored the most points in a single season
SELECT
    player_id,
    season,
    SUM(points) AS total_points
FROM game_details
GROUP BY player_id, season
ORDER BY total_points DESC
LIMIT 1;

-- Identify the team with the most total wins
SELECT
    team_id,
    SUM(CASE WHEN result = 'Win' THEN 1 ELSE 0 END) AS total_wins
FROM game_details
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;