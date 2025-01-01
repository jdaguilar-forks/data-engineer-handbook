SELECT user_id,
       browser_type,
       ARRAY_AGG(DISTINCT event_date) AS device_activity_datelist
FROM events
GROUP BY user_id, browser_type;