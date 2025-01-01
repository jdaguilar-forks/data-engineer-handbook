-- user_devices_cumulated.sql
CREATE TABLE user_devices_cumulated (
    user_id STRING,
    device_activity_datelist MAP<STRING, ARRAY<DATE>>
);