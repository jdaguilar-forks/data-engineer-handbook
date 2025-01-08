INSERT INTO host_activity_reduced (month, host, hit_count, unique_visitors)
SELECT DATE_TRUNC('month', event_date) AS month,
    host_id,
    COUNT(1) AS hit_count,
    ARRAY_AGG(DISTINCT user_id) AS unique_visitors
FROM events
GROUP BY month, host_id;