DO $$
DECLARE
    current_year INT;
BEGIN
    FOR current_year IN 1970..2021 LOOP

		INSERT INTO actors (actor_id, actor, films, quality_class, is_active)
		WITH
		    previous_actors AS (
		        SELECT
		            actor_id,
		            actor,
		            films,
		            quality_class,
		            is_active
		        FROM actors
		    ),
		    current_actors AS (
		        SELECT
		            actorid AS actor_id,
		            actor,
		            ARRAY[]::film_struct[] AS films, -- Initialize empty array for films
		            NULL::quality_classification AS quality_class, -- Placeholder
		            FALSE AS is_active -- Default value
		        FROM actor_films
		        WHERE year = current_year
		    ),
		    union_actors AS (
		        SELECT * FROM previous_actors
		        UNION ALL
		        SELECT * FROM current_actors
		    ),
		    deduplicated_actors AS (
		        SELECT DISTINCT ON (actor_id) -- Deduplicate based on actor_id
		            actor_id,
		            actor,
		            films,
		            quality_class,
		            is_active
		        FROM union_actors
		        ORDER BY actor_id, is_active DESC -- Prefer active rows if duplicates exist
		    ),
		    current_year_actor_films AS (
		        SELECT
		            af.actorid AS actor_id,
		            af.actor,
		            ARRAY_AGG(
		                ROW(
		                    af.year,
		                    af.film,
		                    af.votes,
		                    af.rating,
		                    af.filmid
		                )::film_struct
		            ) AS new_films
		        FROM actor_films af
		        WHERE af.year = current_year
		        GROUP BY af.actorid, af.actor
		    ),
		    avg_rating AS (
		        SELECT
		            actorid AS actor_id,
		            AVG(rating) AS avg_rating
		        FROM actor_films
		        WHERE year = current_year
		        GROUP BY actorid
		    )
		SELECT
		    COALESCE(d.actor_id, f.actor_id) AS actor_id,
		    COALESCE(d.actor, f.actor) AS actor,
		    ARRAY_CAT(
		        d.films,
		        ARRAY(
		            SELECT unnest(f.new_films) -- Unnest the new films
		            EXCEPT
		            SELECT unnest(d.films) -- Remove films already in the existing array
		        )
		    ) AS films, -- Merge old films with only unique new films
		    CASE
		        WHEN r.avg_rating IS NULL THEN d.quality_class -- Retain existing quality_class if no new movies
		        WHEN r.avg_rating > 8 THEN 'star'
		        WHEN r.avg_rating > 7 THEN 'good'
		        WHEN r.avg_rating > 6 THEN 'average'
		        ELSE 'bad'
		    END::quality_classification AS quality_class,
		    CASE
		        WHEN f.actor_id IS NOT NULL THEN TRUE
		        ELSE FALSE
		    END AS is_active
		FROM deduplicated_actors d
		LEFT JOIN current_year_actor_films f ON d.actor_id = f.actor_id
		LEFT JOIN avg_rating r ON d.actor_id = r.actor_id
		ON CONFLICT (actor_id) DO UPDATE
		SET
		    films = EXCLUDED.films,
		    quality_class = EXCLUDED.quality_class,
		    is_active = EXCLUDED.is_active;

    END LOOP;
END $$;