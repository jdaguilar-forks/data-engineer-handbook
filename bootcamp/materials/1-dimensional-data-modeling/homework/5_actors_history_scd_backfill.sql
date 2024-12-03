CREATE TYPE scd_type_film AS (
    quality_class quality_classification,
    is_active boolean,
    start_date INTEGER,
    end_date INTEGER
);


insert into actors_history_scd (
    actor_id,
    quality_class,
    is_active,
    start_date,
    end_date,
    active_date
)

WITH
last_history_scd AS (
    SELECT * FROM actors_history_scd
    WHERE active_date = :previous_year
    AND end_date = :previous_year
),
historical_scd AS (
    select
        actor_id,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE active_date = :previous_year
    AND end_date = :previous_year
),
unnested_films as (
  SELECT actor_id, actor,
         UNNEST(films) as films,
         quality_class,
         is_active
  FROM actors
),
this_films_year AS (
     SELECT
     	uf.*,
     	:current_year as active_date
     FROM unnested_films uf
     WHERE (films::film_struct).year = :current_year
),
unchanged_records AS (
     select
            ts.actor_id,
            ts.quality_class,
            ts.is_active,
            ls.start_date,
            ls.end_date
    FROM this_films_year ts
    JOIN last_history_scd ls
    ON ls.actor_id = ts.actor_id
       WHERE ts.quality_class = ls.quality_class
       AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT
            ts.actor_id, ts.actor,
            UNNEST(ARRAY[
                ROW(
                    ls.quality_class,
                    ls.is_active,
                    ls.start_date,
                    ls.end_date
                )::scd_type_film,
                ROW(
                    ts.quality_class,
                    ts.is_active,
                    ts.active_date,
                    ts.active_date
                )::scd_type_film
            ]) as records
    FROM this_films_year ts
    LEFT JOIN last_history_scd ls
    ON ls.actor_id = ts.actor_id
     WHERE (ts.quality_class <> ls.quality_class
       OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
	 select
	    actor_id,
        (records::scd_type_film).quality_class,
        (records::scd_type_film).is_active,
        (records::scd_type_film).start_date,
        (records::scd_type_film).end_date
        FROM changed_records
),
new_records AS (
     select
        ts.actor_id,
        ts.quality_class,
        ts.is_active,
        ts.active_date AS start_date,
        ts.active_date AS end_date
     FROM this_films_year ts
     LEFT JOIN last_history_scd ls
         ON ts.actor_id = ls.actor_id
     WHERE ls.actor_id IS NULL
)

SELECT *, :current_year AS active_date FROM (
      SELECT *
      FROM historical_scd

      UNION ALL

      SELECT *
      FROM unchanged_records

      UNION ALL

      SELECT *
      FROM unnested_changed_records

      UNION ALL

      SELECT *
      FROM new_records
  ) a