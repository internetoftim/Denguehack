-------------------------------------------------------------------------------
-- Script       : 202_Create_cru_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Climate Research Unit data
-------------------------------------------------------------------------------
CREATE SCHEMA climate;
GRANT CREATE ON SCHEMA climate TO public;
GRANT USAGE ON SCHEMA climate TO public;

CREATE SCHEMA earth;
GRANT CREATE ON SCHEMA earth TO public;
GRANT USAGE ON SCHEMA earth TO public;
CREATE SCHEMA life;
GRANT CREATE ON SCHEMA life TO ts186045;
GRANT USAGE ON SCHEMA life TO public;

DROP TABLE IF EXISTS climate.cru_ts3_pre;

CREATE DIMENSION TABLE climate.cru_ts3_pre
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);

GRANT SELECT ON TABLE climate.cru_ts3_pre TO public;


DROP TABLE IF EXISTS climate.cru_ts3_wet;

CREATE DIMENSION TABLE climate.cru_ts3_wet
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_wet TO public;



DROP TABLE IF EXISTS climate.cru_ts3_cld;

CREATE DIMENSION TABLE climate.cru_ts3_cld
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_cld TO public;


DROP TABLE IF EXISTS climate.cru_ts3_dtr;

CREATE DIMENSION TABLE climate.cru_ts3_dtr
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_dtr TO public;


DROP TABLE IF EXISTS climate.cru_ts3_frs;

CREATE DIMENSION TABLE climate.cru_ts3_frs
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_frs TO public;


DROP TABLE IF EXISTS climate.cru_ts3_tmn;

CREATE DIMENSION TABLE climate.cru_ts3_tmn
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_tmn TO public;


DROP TABLE IF EXISTS climate.cru_ts3_tmp;

CREATE DIMENSION TABLE climate.cru_ts3_tmp
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_tmp TO public;


DROP TABLE IF EXISTS climate.cru_ts3_tmx;

CREATE DIMENSION TABLE climate.cru_ts3_tmx
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_tmx TO public;


DROP TABLE IF EXISTS climate.cru_ts3_vap;

CREATE DIMENSION TABLE climate.cru_ts3_vap
(   rowid SERIAL GLOBAL
    ,data VARCHAR	
);
GRANT SELECT ON TABLE climate.cru_ts3_vap TO public;



ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_pre cru_ts3.23.1901.2014.pre.dat -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_cld cru_ts3.23.1901.2014.cld.dat  -C "data"

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_wet cru_ts3.23.01.1901.2014.wet.dat -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_dtr cru_ts3.23.1901.2014.dtr.dat  -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_frs cru_ts3.23.1901.2014.frs.dat  -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_tmn cru_ts3.23.1901.2014.tmn.dat  -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_tmp cru_ts3.23.1901.2014.tmp.dat  -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_tmx cru_ts3.23.1901.2014.tmx.dat  -C "data"
ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors climate.cru_ts3_vap cru_ts3.23.1901.2014.vap.dat  -C "data"

-------------------------------------------------------------------------------
-- Inspect ASC raw files
-------------------------------------------------------------------------------
SELECT * FROM climate.cru_ts3_pre LIMIT 7;
SELECT * FROM climate.cru_ts3_wet LIMIT 7;
SELECT * FROM climate.cru_ts3_cld LIMIT 7;	
SELECT * FROM climate.cru_ts3_dtr LIMIT 7;
SELECT * FROM climate.cru_ts3_frs LIMIT 7;
SELECT * FROM climate.cru_ts3_tmn LIMIT 7;
SELECT * FROM climate.cru_ts3_tmp LIMIT 7;
SELECT * FROM climate.cru_ts3_tmx LIMIT 7;
SELECT * FROM climate.cru_ts3_vap LIMIT 7;




DROP TABLE IF EXISTS earth.cru_ts3_wet;
CREATE DIMENSION TABLE earth.cru_ts3_wet AS
SELECT * FROM climate.cru_ts3_wet;
GRANT SELECT ON TABLE earth.cru_ts3_wet TO public;


DROP TABLE IF EXISTS earth.cru_ts3_pre;
CREATE DIMENSION TABLE earth.cru_ts3_pre AS
SELECT * FROM climate.cru_ts3_pre;
GRANT SELECT ON TABLE earth.cru_ts3_pre TO public;


DROP TABLE IF EXISTS earth.cru_ts3_cld;
CREATE DIMENSION TABLE earth.cru_ts3_cld AS
SELECT * FROM climate.cru_ts3_cld;
GRANT SELECT ON TABLE earth.cru_ts3_cld TO public;


DROP TABLE IF EXISTS earth.cru_ts3_dtr;
CREATE DIMENSION TABLE earth.cru_ts3_dtr AS
SELECT * FROM climate.cru_ts3_dtr;
GRANT SELECT ON TABLE earth.cru_ts3_dtr TO public;


DROP TABLE IF EXISTS earth.cru_ts3_frs;
CREATE DIMENSION TABLE earth.cru_ts3_frs AS
SELECT * FROM climate.cru_ts3_frs;
GRANT SELECT ON TABLE earth.cru_ts3_frs TO public;


DROP TABLE IF EXISTS earth.cru_ts3_tmn;
CREATE DIMENSION TABLE earth.cru_ts3_tmn AS
SELECT * FROM climate.cru_ts3_tmn;
GRANT SELECT ON TABLE earth.cru_ts3_tmn TO public;


DROP TABLE IF EXISTS earth.cru_ts3_tmp;
CREATE DIMENSION TABLE earth.cru_ts3_tmp AS
SELECT * FROM climate.cru_ts3_tmp;
GRANT SELECT ON TABLE earth.cru_ts3_tmp TO public;


DROP TABLE IF EXISTS earth.cru_ts3_tmx;
CREATE DIMENSION TABLE earth.cru_ts3_tmx AS
SELECT * FROM climate.cru_ts3_tmx;
GRANT SELECT ON TABLE earth.cru_ts3_tmx TO public;

DROP TABLE IF EXISTS earth.cru_ts3_vap;
CREATE DIMENSION TABLE earth.cru_ts3_vap AS
SELECT * FROM climate.cru_ts3_vap;
GRANT SELECT ON TABLE earth.cru_ts3_vap;



GRANT SELECT ON TABLE earth.cru_ts3_wet TO public;

GRANT SELECT ON TABLE earth.cru_ts3_pre TO public;

GRANT SELECT ON TABLE earth.cru_ts3_cld TO public;
GRANT SELECT ON TABLE earth.cru_ts3_dtr TO public;
GRANT SELECT ON TABLE earth.cru_ts3_frs TO public;

GRANT SELECT ON TABLE earth.cru_ts3_tmn TO public;

GRANT SELECT ON TABLE earth.cru_ts3_tmp TO public;
GRANT SELECT ON TABLE earth.cru_ts3_tmx TO public;
GRANT SELECT ON TABLE earth.cru_ts3_vap;


