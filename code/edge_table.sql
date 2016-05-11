DROP TABLE IF EXISTS edge_table;

--EDGE TABLE CREATE
CREATE TABLE edge_table (
    id BIGSERIAL,
    source BIGINT,
    target BIGINT,
    cost FLOAT,
    x1 FLOAT,
    y1 FLOAT,
    x2 FLOAT,
    y2 FLOAT,
    the_geom geometry
);

--EDGE TABLE ADD DATA
INSERT INTO edge_table (id,source,target,cost,x1,y1,x2,y2) 
VALUES ( 0, 1, 2, 1, 0, 0, 1, 1);
INSERT INTO edge_table (id,source,target,cost,x1,y1,x2,y2) 
VALUES ( 1, 2, 4, 1, 1, 1, 1, 2);
INSERT INTO edge_table (id,source,target,cost,x1,y1,x2,y2) 
VALUES ( 2, 2, 5, 1, 1, 1, 2, 2);
INSERT INTO edge_table (id,source,target,cost,x1,y1,x2,y2) 
VALUES ( 3, 2, 6, 1, 1, 1, 2, 0);

--UPDATING EDGE GEOMETRIES
UPDATE edge_table SET the_geom = st_makeline(st_point(x1,y1),st_point(x2,y2));

--ADDING A NEW COLUMN TO STORE THE STROKE ID TO WHICH TH EDGE BELONGS
ALTER TABLE edge_table ADD COLUMN stroke_ids integer ARRAY DEFAULT '{}';

--EDGE TABLE TOPOLOGY
SELECT pgr_createTopology('edge_table',0.001);




