-- Materialized view caches results since the query inside is ridiculously expensive
CREATE MATERIALIZED VIEW hops_aggregate AS (
    SELECT
        agg.src,
        src_loc.lat AS src_lat,
        src_loc.lng AS src_lng,
        agg.dst,
        dst_loc.lat AS dst_lat,
        dst_loc.lng AS dst_lng,
        agg.rtt_avg,
        agg.rtt_stdev,
        agg.rtt_range,
        agg.measurments
    FROM (
             SELECT src,
                    dst,
                    AVG(RTT)            AS rtt_avg,
                    stddev(RTT)         AS rtt_stdev,
                    MAX(RTT) - MIN(RTT) AS rtt_range,
                    COUNT(src)          AS measurments
             FROM hops
             GROUP BY (src, dst)
         ) agg
             INNER JOIN locations src_loc
                        ON agg.src = src_loc.ip
                            AND src_loc.lat != 'NaN'::float
             INNER JOIN locations dst_loc
                        ON agg.src = dst_loc.ip
                            AND dst_loc.lat != 'NaN'::float
);
CREATE INDEX hops_aggregate_src_loc_index ON hops_aggregate(src_lat, src_lng);
CREATE INDEX hops_aggregate_dst_loc_index ON hops_aggregate(dst_lat, dst_lng);
REFRESH MATERIALIZED VIEW hops_aggregate; -- Run to update the view. Will take a while!

