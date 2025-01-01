SELECT user_id,
       browser_type,
       ARRAY_TRANSFORM(device_activity_datelist, date -> CAST(date AS INT)) AS datelist_int
FROM user_devices_cumulated;