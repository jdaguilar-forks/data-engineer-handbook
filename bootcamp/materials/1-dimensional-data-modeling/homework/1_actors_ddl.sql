CREATE TYPE quality_classification AS
ENUM ('star', 'good', 'average', 'bad');

CREATE TYPE film_struct AS (
    year INT,
    film TEXT,
    votes INT,
    rating FLOAT,
    film_id TEXT
);

CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    films film_struct[],
    quality_class quality_classification,
    is_active BOOLEAN,
    PRIMARY KEY(actor_id)
);
