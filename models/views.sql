CREATE OR REPLACE FUNCTION haversine_distance (lat1 FLOAT, lng1 FLOAT, lat2 FLOAT, lng2 FLOAT)
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

-- Materialized view caches results since the query inside is ridiculously expensive

CREATE MATERIALIZED VIEW hops_aggregate AS (
    SELECT
        agg.src,
        agg.dst,
        agg.indirect,
        src_loc.coord as src_loc,
        dst_loc.coord as dst_loc,
        haversine_distance(src_loc.coord[0], src_loc.coord[1], dst_loc.coord[0], dst_loc.coord[1]) AS distance,
        agg.rtt_avg,
        agg.rtt_stdev,
        agg.rtt_range,
        agg.time_avg,
        agg.time_stdev,
        agg.time_range,
        agg.measurements
    FROM (
             SELECT src,
                    dst,
                    indirect,
                    AVG(RTT)                AS rtt_avg,
                    STDDEV(RTT)             AS rtt_stdev,
                    MAX(RTT) - MIN(RTT)     AS rtt_range,
                    AVG(time)               AS time_avg,
                    STDDEV(time)            AS time_stdev,
                    MAX(time) - MIN(time)   AS time_range,
                    COUNT(src)              AS measurements
             FROM hops
             GROUP BY (dst, src, indirect)
         ) agg
    INNER JOIN locations src_loc ON agg.src = src_loc.ip
    INNER JOIN locations dst_loc ON agg.dst = dst_loc.ip
);
CREATE TABLE hops_aggregate AS (SELECT * FROM hops_aggregate_view);

CREATE INDEX hops_aggregate_src_index ON hops_aggregate(src);
CREATE INDEX hops_aggregate_dst_index ON hops_aggregate(dst);
CREATE INDEX hops_aggregate_avg_index ON hops_aggregate USING BRIN(rtt_avg);
-- REFRESH MATERIALIZED VIEW hops_aggregate; -- Run to update the view. Will take a while!

CREATE VIEW hops_aggregate_stdev_filtered AS (
    WITH bounds AS (
        SELECT (AVG(rtt_avg) - STDDEV_SAMP(rtt_avg) * 0.2) AS lower_bound,
               (AVG(rtt_avg) + STDDEV_SAMP(rtt_avg) * 1.0) AS upper_bound
        FROM hops_aggregate
    )
    SELECT * FROM hops_aggregate WHERE rtt_avg BETWEEN (SELECT lower_bound FROM bounds) AND (SELECT upper_bound FROM bounds)
);

CREATE TABLE hops_aggregate_us (
    src             INET,
    dst             INET,
    indirect        BOOLEAN,
    src_loc         POINT,
    dst_loc         POINT,
    distance        REAL,
    rtt_avg         REAL,
    rtt_stdev       REAL,
    rtt_range       REAL,
    time_avg        REAL,
    time_stdev      REAL,
    time_range      REAL,
    measurements    BIGINT,
    PRIMARY KEY (src, dst, indirect)
);
INSERT INTO hops_aggregate_us (
    SELECT *
    FROM hops_aggregate
    WHERE BOX(POINT(18.91619, -171.791110603), POINT(71.3577635769, -66.96466)) @> src_loc
    OR BOX(POINT(18.91619, -171.791110603), POINT(71.3577635769, -66.96466)) @> dst_loc
);

CREATE INDEX hops_aggregate_us_src_index ON hops_aggregate_us(src);
CREATE INDEX hops_aggregate_us_dst_index ON hops_aggregate_us(dst);
CREATE INDEX hops_aggregate_us_avg_index ON hops_aggregate_us USING BRIN(rtt_avg);
CREATE INDEX hops_aggregate_us_spatial_src_index ON hops_aggregate_us USING spgist(src_loc);
CREATE INDEX hops_aggregate_us_spatial_dst_index ON hops_aggregate_us USING spgist(dst_loc);
SELECT COUNT(*) FROM hops_aggregate_us;

