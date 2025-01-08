-- Average number of web events per session for Tech Creator
SELECT AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io';

-- Average number of web events per session for different hosts
SELECT host, AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host;
