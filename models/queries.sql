SELECT count(src) FROM hops;
SELECT count(ip) FROM locations;
SELECT count(src) FROM hops_aggregate;

INSERT INTO locations(ip)
    SELECT DISTINCT src FROM hops
ON CONFLICT DO NOTHING;

INSERT INTO locations(ip)
    SELECT DISTINCT dst FROM hops
ON CONFLICT DO NOTHING;

SELECT ip FROM locations WHERE lat IS NULL or lng IS NUll;

SELECT *FROM hops_aggregate WHERE measurments > 50 AND rtt_avg >0 ORDER BY rtt_stdev, measurments DESC;

-- DELETE FROM hops;

SELECT dst_lat AS lat, dst_lng AS lng, rtt_avg AS rtt FROM hops_aggregate WHERE rtt_avg < 750;

SELECT src, AVG(measurements) FROM hops_aggregate  WHERE src_lat < 71.35 AND src_lat > 18.91 AND src_lng > -171.79 AND src_lng < -66.96
GROUP BY src;

SELECT src, AVG(rtt_avg) FROM hops_aggregate GROUP BY src;

SELECT COUNT(DISTINCT dst) FROM hops_aggregate;

WHERE rtt_avg > 5 AND rtt_avg < 2000 AND distance > 0
SELECT dst, dst_lat AS lat, dst_lng AS lng, AVG(rtt_avg / distance) * 100 AS dist
FROM hops_aggregate
WHERE rtt_avg > 5 AND rtt_avg < 2000 AND distance > 0
GROUP BY dst, dst_lat, dst_lng;

SELECT COUNT(lng) FROM hops_ms_per_km WHERE lat = -97;

SELECT SUM(reltuples) AS approximate_row_count FROM pg_class WHERE relname LIKE 'h%';

VACUUM VERBOSE ANALYZE hops;

DELETE FROM hops;
