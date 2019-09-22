CREATE OR REPLACE FUNCTION haversine_distance (lat1 REAL, lng1 REAL, lat2 REAL, lng2 REAL)
RETURNS REAL AS $distance$
    DECLARE ph1 REAL := radians(lat1);
    DECLARE ph2 REAL := radians(lat2);
    DECLARE dph REAL := ph2 - ph1;
    DECLARE dld REAL := RADIANS(lng2 - lng1);
    DECLARE a REAL := (POW(SIN(dph/2), 2) + COS(ph1) * COS(ph2) * POW(SIN(dld/2), 2));
    BEGIN
        RETURN 6371 * 2 * ATAN2(SQRT(a), SQRT(1-a));
    END;
$distance$ LANGUAGE plpgsql;

SELECT haversine_distance(-180, 174, -41, 180);

-- Materialized view caches results since the query inside is ridiculously expensive
CREATE MATERIALIZED VIEW hops_aggregate AS (
    SELECT
        agg.src,
        src_loc.lat AS src_lat,
        src_loc.lng AS src_lng,
        agg.dst,
        dst_loc.lat AS dst_lat,
        dst_loc.lng AS dst_lng,
        haversine_distance(src_loc.lat, src_loc.lng, dst_loc.lat, dst_loc.lng) AS distance,
        agg.rtt_avg,
        agg.rtt_stdev,
        agg.rtt_range,
        agg.measurements
    FROM (
             SELECT src,
                    dst,
                    AVG(RTT)            AS rtt_avg,
                    stddev(RTT)         AS rtt_stdev,
                    MAX(RTT) - MIN(RTT) AS rtt_range,
                    COUNT(src)          AS measurements
             FROM hops
             GROUP BY (src, dst)
         ) agg
             INNER JOIN locations src_loc
                        ON agg.src = src_loc.ip
                            AND src_loc.lat != 'NaN'::float
             INNER JOIN locations dst_loc
                        ON agg.dst = dst_loc.ip
                            AND dst_loc.lat != 'NaN'::float
);
CREATE INDEX hops_aggregate_src_loc_index ON hops_aggregate(src_lat, src_lng);
CREATE INDEX hops_aggregate_dst_loc_index ON hops_aggregate(dst_lat, dst_lng);
CREATE INDEX hops_aggregate_src_index ON hops_aggregate(src);
CREATE INDEX hops_aggregate_dst_index ON hops_aggregate(dst);
CREATE INDEX hops_aggregate_avg_index ON hops_aggregate(rtt_avg);
REFRESH MATERIALIZED VIEW hops_aggregate; -- Run to update the view. Will take a while!

CREATE VIEW hops_aggregate_stdev_filtered AS (
    WITH bounds AS (
        SELECT (AVG(rtt_avg) - STDDEV_SAMP(rtt_avg) * 0.2) AS lower_bound,
               (AVG(rtt_avg) + STDDEV_SAMP(rtt_avg) * 1.0) AS upper_bound
        FROM hops_aggregate
    )
    SELECT * FROM hops_aggregate WHERE rtt_avg BETWEEN (SELECT lower_bound FROM bounds) AND (SELECT upper_bound FROM bounds)
);

CREATE MATERIALIZED VIEW hops_ms_per_km AS (
   SELECT dst, dst_lat AS lat, dst_lng AS lng, AVG(rtt_avg / distance) AS connectivity
    FROM hops_aggregate
    WHERE rtt_avg > 5 AND rtt_avg < 2000 AND distance > 0
    GROUP BY dst, dst_lat, dst_lng
);
CREATE INDEX hops_ms_per_km_pos_index ON hops_ms_per_km(lat, lng);
CREATE INDEX hops_ms_per_km_connectivity_index ON hops_ms_per_km(connectivity);
