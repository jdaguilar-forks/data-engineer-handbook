WITH team_wins AS (
    SELECT
        team_id,
        game_date,
        CASE WHEN result = 'Win' THEN 1 ELSE 0 END AS win
    FROM game_details
),
cumulative_wins AS (
    SELECT
        team_id,
        game_date,
        SUM(win) OVER (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_90_games
    FROM team_wins
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM cumulative_wins
GROUP BY team_id;



WITH lebron_games AS (
    SELECT
        player_id,
        game_date,
        points,
        CASE WHEN points > 10 THEN 1 ELSE 0 END AS over_10_points
    FROM game_details
    WHERE player_id = (SELECT player_id FROM players WHERE player_name = 'LeBron James')
),
streaks AS (
    SELECT
        game_date,
        points,
        over_10_points,
        SUM(CASE WHEN over_10_points = 0 THEN 1 ELSE 0 END) OVER (ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_id
    FROM lebron_games
)
SELECT
    MAX(COUNT(*)) AS max_streak
FROM streaks
WHERE over_10_points = 1
GROUP BY streak_id;
