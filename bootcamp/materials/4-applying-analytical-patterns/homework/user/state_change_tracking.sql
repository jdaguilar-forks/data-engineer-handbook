WITH player_changes AS (
    SELECT
        player_id,
        season,
        LAG(season) OVER (PARTITION BY player_id ORDER BY season) AS prev_season,
        LAG(status) OVER (PARTITION BY player_id ORDER BY season) AS prev_status,
        status
    FROM player_seasons
)
SELECT
    player_id,
    season,
    CASE
        WHEN prev_season IS NULL THEN 'New'
        WHEN status = 'Active' AND prev_status = 'Retired' THEN 'Returned from Retirement'
        WHEN status = 'Retired' AND prev_status = 'Active' THEN 'Retired'
        WHEN status = 'Active' AND prev_status = 'Active' THEN 'Continued Playing'
        WHEN status = 'Retired' AND prev_status = 'Retired' THEN 'Stayed Retired'
    END AS state_change
FROM player_changes;