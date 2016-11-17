-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Population GPWV4 data
-------------------------------------------------------------------------------
CREATE SCHEMA social;
GRANT CREATE ON SCHEMA social TO public;
GRANT USAGE ON SCHEMA social TO public;

CREATE SCHEMA ts186045;
GRANT CREATE ON SCHEMA social TO ts186045;
GRANT USAGE ON SCHEMA social TO ts186045;

DROP TABLE IF EXISTS social.gpwv4_2005;

CREATE DIMENSION TABLE social.gpwv4_2005
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE social.gpwv4_2005 TO public;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors social.gpwv4_2005  gpwv4_2005.asc -C "data"



DROP TABLE IF EXISTS social.gpwv4_2000;

CREATE DIMENSION TABLE social.gpwv4_2000
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE social.gpwv4_2000 TO public;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors social.gpwv4_2000  gpwv4_2000.asc -C "data"


DROP TABLE IF EXISTS social.gpwv4_2015;

CREATE DIMENSION TABLE social.gpwv4_2015
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE social.gpwv4_2015 TO public;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors social.gpwv4_2015  gpwv4_2015.asc -C "data"


DROP TABLE IF EXISTS social.gpwv4_2010;

CREATE DIMENSION TABLE social.gpwv4_2010
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE social.gpwv4_2010 TO public;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors social.gpwv4_2010  gpwv4_2010.asc -C "data"


DROP TABLE IF EXISTS social.gpwv4_2020;

CREATE DIMENSION TABLE social.gpwv4_2020
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE social.gpwv4_2020 TO public;

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors social.gpwv4_2020  gpwv4_2020.asc -C "data"

-------------------------------------------------------------------------------
-- Inspect ASC raw files
-------------------------------------------------------------------------------
SELECT * FROM social.gpwv4_2000 LIMIT 7;
SELECT * FROM social.gpwv4_2005 LIMIT 7;
SELECT * FROM social.gpwv4_2010 LIMIT 10;
SELECT * FROM social.gpwv4_2015 LIMIT 7;
SELECT * FROM social.gpwv4_2020 LIMIT 7;;

select * from social.gpwv4_xyz_raw where rowid=0 limit 6
select count(*) from social.gpwv4_2000 where sn=0;
select count(*) from social.gpwv4_2020 where rowid=0