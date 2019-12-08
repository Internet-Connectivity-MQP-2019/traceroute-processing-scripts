EXPLAIN SELECT count(*) FROM hops;
SELECT count(ip) FROM locations;
SELECT count(src) FROM hops_aggregate;

SELECT hop_split(inet_in('192.168.1.1'), inet_in('192.168.1.2'));

INSERT INTO locations(ip)
    (SELECT dst FROM hops)
ON CONFLICT DO NOTHING;

INSERT INTO locations(ip)
    (SELECT src FROM hops)
ON CONFLICT DO NOTHING;

SELECT COUNT(*) FROM locations;

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
SELECT * FROM pg_class WHERE relname LIKE 'h%' ORDER BY reltuples ASC;

SELECT
  t.tablename,
  indexname,
  c.reltuples AS num_rows,
  pg_size_pretty(pg_relation_size(quote_ident(t.tablename)::text)) AS table_size,
  pg_size_pretty(pg_relation_size(quote_ident(indexrelname)::text)) AS index_size,
  CASE WHEN indisunique THEN 'Y'
    ELSE 'N'
  END AS UNIQUE,
  idx_scan AS number_of_scans,
  idx_tup_read AS tuples_read,
  idx_tup_fetch AS tuples_fetched
FROM pg_tables t
  LEFT OUTER JOIN pg_class c ON t.tablename=c.relname
  LEFT OUTER JOIN
    ( SELECT c.relname AS ctablename, ipg.relname AS indexname, x.indnatts AS number_of_columns, idx_scan, idx_tup_read, idx_tup_fetch, indexrelname, indisunique FROM pg_index x
      JOIN pg_class c ON c.oid = x.indrelid
      JOIN pg_class ipg ON ipg.oid = x.indexrelid
      JOIN pg_stat_all_indexes psai ON x.indexrelid = psai.indexrelid )
    AS foo
  ON t.tablename = foo.ctablename
WHERE t.schemaname='public'
ORDER BY 1,2;

-- Wyoming!
SELECT src FROM hops_aggregate WHERE
    src_loc[1] >= -111.05688 AND
    src_loc[0] >= 40.994746 AND
    src_loc[1] <= -104.05216 AND
    src_loc[0] <= 45.005904
ORDER BY src;

SELECT src, src_loc[0], src_loc[1], AVG(rtt_avg / distance) AS connectivity FROM hops_aggregate
WHERE distance != 0 AND (rtt_avg / distance) < 0.1 AND (rtt_avg / distance) > 0.01 AND indirect = FALSE
  AND BOX(POINT(-45, -90), POINT(-22.5, -45)) @> src_loc
GROUP BY (src, src_loc[0], src_loc[1]);

SELECT COUNT(*) FROM hops_aggregate;
SELECT COUNT(*) FROM hops_aggregate WHERE indirect;
SELECT COUNT(*) FROM hops_aggregate WHERE !indirect;
DELETE FROM quads;

SELECT src_loc[0] AS src_lat, src_loc[1] AS src_lng, dst_loc[0] AS dst_lat, dst_loc[1] AS dst_lng, indirect::int, rtt_avg, rtt_stdev, rtt_range, measurements FROM  hops_aggregate_us WHERE rtt_avg > 0

SELECT *, frac_c_efficiency(rtt_avg, distance) FROM hops_aggregate_us
WHERE
    NOT indirect
    AND rtt_avg > 0
    AND distance > 0
    AND frac_c_efficiency(rtt_avg, distance) < 1.0
ORDER BY rtt_avg LIMIT 500;

SELECT COUNT(*) FROM hops_aggregate_view WHERE distance > 0 AND rtt_avg > 0 AND frac_c_efficiency(rtt_avg, distance) < 1;

SELECT src_lat, src_lng, dst_lat, dst_lng, distance, rtt_avg, rtt_stdev, rtt_range, measurements, frac_c_efficiency(rtt_avg, distance) FROM hops_aggregate_us WHERE distance > 0 AND frac_c_efficiency(rtt_avg, distance) < 1 AND frac_c_efficiency(rtt_avg, distance) >= 0 AND rtt_avg > 0 AND NOT indirect;


SELECT COUNT(*) FROM hops_aggregate_us
WHERE distance > 0 AND frac_c_efficiency(rtt_avg, distance) < 1 AND frac_c_efficiency(rtt_avg, distance) >= 0 AND rtt_avg > 0
