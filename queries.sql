SELECT count(src) FROM hops;
SELECT count(ip) FROM locations;

INSERT INTO locations(ip)
    SELECT DISTINCT src FROM hops
ON CONFLICT DO NOTHING;

INSERT INTO locations(ip)
    SELECT DISTINCT dst FROM hops
ON CONFLICT DO NOTHING;

SELECT ip FROM locations WHERE lat IS NULL or lng IS NUll;

SELECT *FROM hops_aggregate WHERE measurments > 50 AND rtt_avg >0 ORDER BY rtt_stdev, measurments DESC;

DELETE FROM hops;