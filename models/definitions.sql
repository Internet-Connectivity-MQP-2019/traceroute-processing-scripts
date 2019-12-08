-- Hops table: One source, one destination, one RTT. Partition by first 4 bits of source, index by source and hops.
-- Timestamp provides info on when the measurement was taken, "indirect" indicates this hop's RTT was calculated.
CREATE TABLE hops (
    src         INET,
    dst         INET,
    rtt         REAL
);
CREATE INDEX src_index ON he USING HASH(src);
CREATE INDEX dst_src_BRIN_index ON hops USING brin(dst, src);
VACUUM VERBOSE ANALYSE hz;

CREATE TABLE h0 PARTITION OF hops FOR VALUES FROM ('0.0.0.0') TO ('16.255.255.255');
CREATE TABLE h1 PARTITION OF hops FOR VALUES FROM ('16.255.255.255') TO ('32.255.255.255');
CREATE TABLE h2 PARTITION OF hops FOR VALUES FROM ('32.255.255.255') TO ('48.255.255.255');
CREATE TABLE h3 PARTITION OF hops FOR VALUES FROM ('48.255.255.255') TO ('64.255.255.255');
CREATE TABLE h4 PARTITION OF hops FOR VALUES FROM ('64.255.255.255') TO ('80.255.255.255');
CREATE TABLE h5 PARTITION OF hops FOR VALUES FROM ('80.255.255.255') TO ('96.255.255.255');
CREATE TABLE h6 PARTITION OF hops FOR VALUES FROM ('96.255.255.255') TO ('112.255.255.255');
CREATE TABLE h7 PARTITION OF hops FOR VALUES FROM ('112.255.255.255') TO ('128.255.255.255');
CREATE TABLE h8 PARTITION OF hops FOR VALUES FROM ('128.255.255.255') TO ('144.255.255.255');
CREATE TABLE h9 PARTITION OF hops FOR VALUES FROM ('144.255.255.255') TO ('160.255.255.255');
CREATE TABLE hA PARTITION OF hops FOR VALUES FROM ('160.255.255.255') TO ('176.255.255.255');
CREATE TABLE hB PARTITION OF hops FOR VALUES FROM ('176.255.255.255') TO ('192.255.255.255');
CREATE TABLE hC PARTITION OF hops FOR VALUES FROM ('192.255.255.255') TO ('208.255.255.255');
CREATE TABLE hD PARTITION OF hops FOR VALUES FROM ('208.255.255.255') TO ('224.255.255.255');
CREATE TABLE hE PARTITION OF hops FOR VALUES FROM ('224.255.255.255') TO ('240.255.255.255');
CREATE TABLE hF PARTITION OF hops FOR VALUES FROM ('240.255.255.255') TO ('255.255.255.255');
CREATE TABLE hZ PARTITION OF hops FOR VALUES FROM ('::') TO ('FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF');

-- Locations table: One IP, one coordinate pair. Partition by first 4 bits of the IP, index by IP and location.
CREATE TABLE locations (
    ip INET PRIMARY KEY,
    coord POINT -- Lat/Long
);
CREATE INDEX ip_index ON locations USING HASH(ip);
CREATE INDEX ip_index2 ON locations(ip ASC);

CREATE TABLE unlocatable_ips (
    ip INET PRIMARY KEY
);
INSERT INTO unlocatable_ips (SELECT ip FROM locations WHERE coord[0] = 'NaN'::float);
DELETE FROM locations WHERE coord[0] = 'NaN'::float;

CREATE TABLE hops_aggregate (
    src             INET,
    dst             INET,
    indirect        BOOLEAN,
    src_loc         POINT,
    dst_loc         POINT,
    distance        REAL,
    rtt_avg         REAL,
    rtt_stdev       REAL,
    rtt_range       REAL,
    measurements    BIGINT,
    PRIMARY KEY (src, dst, indirect)
);

CREATE INDEX hops_stats_index ON hops_aggregate_view USING brin(rtt_avg, rtt_stdev, rtt_range, measurements);
CREATE INDEX hops_spatial_src_index ON hops_aggregate_view USING spgist(src_loc);
CREATE INDEX hops_spatial_dst_index ON hops_aggregate_view USING spgist(dst_loc);


CREATE OR REPLACE FUNCTION public.hashpoint(point) RETURNS INTEGER
    LANGUAGE sql IMMUTABLE
    AS 'SELECT hashfloat8($1[0]) # hashfloat8($1[1])';

CREATE OPERATOR CLASS public.point_hash_ops DEFAULT FOR TYPE POINT USING hash AS
    OPERATOR 1 ~=(point,point),
    FUNCTION 1 public.hashpoint(point);
