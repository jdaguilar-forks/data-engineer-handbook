CREATE TABLE IF NOT EXISTS actors_history_scd (
    actor_id TEXT NOT NULL,
    quality_class quality_classification,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    active_date INTEGER
);
