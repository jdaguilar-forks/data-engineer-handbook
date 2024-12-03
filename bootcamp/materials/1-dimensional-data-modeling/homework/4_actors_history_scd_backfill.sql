
insert into actors_history_scd (
    actor_id,
    quality_class,
    is_active,
    start_date,
    end_date,
    active_date
)
with
unnested_films as (
  SELECT actor_id,
         UNNEST(films) as films
  FROM actors
),
unnested_grouped_films as (
  SELECT actor_id,
         max( (films::film_struct).year ) as max_year,
         min( (films::film_struct).year ) as min_year
  FROM unnested_films
  group by actor_id
),
actors_status as (
  SELECT actor_id,
         quality_class,
         is_active
  FROM actors
)
select
	ast.actor_id,
	ast.quality_class,
    ast.is_active,
    ugf.min_year as start_date,
    ugf.max_year as end_date,
    ( SELECT max((films::film_struct).year) FROM unnested_films) as active_date
from unnested_grouped_films ugf
inner join actors_status ast
	on ugf.actor_id = ast.actor_id
