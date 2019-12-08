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

CREATE OR REPLACE FUNCTION frac_c_efficiency (rtt FLOAT, distance FLOAT)
RETURNS REAL AS $efficiency$
    BEGIN
        RETURN (2 * distance) / (rtt * 299.79246);
    END;
$efficiency$ LANGUAGE plpgsql;

CREATE MATERIALIZED VIEW hops_stats AS (
    SELECT
            src,
            dst,
            indirect,
            avg(rtt),
            CASE
                WHEN COUNT(*) = 1 THEN 9999999
                ELSE stddev_samp(rtt)
                END
        FROM hops
        GROUP BY src, dst, indirect
);
CREATE INDEX hops_stats_view_index ON hops_stats(src, dst, indirect);

CREATE MATERIALIZED VIEW hops_aggregate_view AS (
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
        agg.measurements
    FROM (
             SELECT hops.src,
                    hops.dst,
                    hops.indirect,
                    AVG(RTT)                AS rtt_avg,
                    STDDEV_SAMP(RTT)        AS rtt_stdev,
                    MAX(RTT) - MIN(RTT)     AS rtt_range,
                    COUNT(*)              AS measurements
             FROM hops
                      INNER JOIN hops_stats hs
                                 ON hops.src = hs.src AND hops.dst = hs.dst AND hs.stddev_samp != 0 AND hops.indirect = hs.indirect AND ABS((hops.rtt - hs.avg) / hs.stddev_samp) <= 2
             GROUP BY (hops.dst, hops.src, hops.indirect)
         ) agg
             INNER JOIN locations src_loc ON agg.src = src_loc.ip
             INNER JOIN locations dst_loc ON agg.dst = dst_loc.ip
);



CREATE TABLE hops_aggregate AS (SELECT * FROM hops_aggregate_view);
CREATE INDEX hops_aggregate_src_index ON hops_aggregate(src);
CREATE INDEX hops_aggregate_dst_index ON hops_aggregate(dst);
CREATE INDEX hops_aggregate_avg_index ON hops_aggregate USING BRIN(rtt_avg);
-- REFRESH MATERIALIZED VIEW hops_aggregate; -- Run to update the view. Will take a while!

CREATE TABLE hops_aggregate_us (
    src             INET,
    dst             INET,
    indirect        BOOLEAN,
    src_lat         REAL,
    src_lng         REAL,
    dst_lat         REAL,
    dst_lng         REAL,
    distance        REAL,
    rtt_avg         REAL,
    rtt_stdev       REAL,
    rtt_range       REAL,
    measurements    BIGINT,
    PRIMARY KEY (src, dst, indirect)
);
INSERT INTO hops_aggregate_us (
    SELECT
           src,
           dst,
           indirect,
           src_loc[0],
           src_loc[1],
           dst_loc[0],
           dst_loc[1],
           distance,
           rtt_avg,
           rtt_stdev,
           rtt_range,
           measurements
    FROM hops_aggregate_view
    WHERE BOX(POINT(18.91619, -171.791110603), POINT(71.3577635769, -66.96466)) @> src_loc
    OR BOX(POINT(18.91619, -171.791110603), POINT(71.3577635769, -66.96466)) @> dst_loc
);

CREATE INDEX hops_aggregate_us_src_dst_index ON hops_aggregate_us(src, dst);
CREATE INDEX hops_aggregate_us_stats_index ON hops_aggregate_us USING BRIN(rtt_avg, rtt_stdev, rtt_range, measurements);
CREATE INDEX hops_aggregate_us_spatial_src_index ON hops_aggregate_us USING spgist(src_loc);
CREATE INDEX hops_aggregate_us_spatial_dst_index ON hops_aggregate_us USING spgist(dst_loc);
SELECT COUNT(*) FROM hops_aggregate_view;
SELECT SUM(measurements) FROM hops_aggregate_view;

