INSERT INTO hosts_cumulated (host_id, host_activity_datelist)
SELECT host_id,
       ARRAY_AGG(DISTINCT event_date)
FROM events
WHERE event_date >= CURRENT_DATE - INTERVAL '1 DAY'
GROUP BY host_id;