-- deduplicate_game_details.sql

WITH game_details_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id ORDER BY some_column) as row_num
    FROM game_details
)
SELECT * FROM game_details_deduped WHERE row_num = 1;
