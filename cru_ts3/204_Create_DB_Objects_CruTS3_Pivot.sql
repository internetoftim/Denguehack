-------------------------------------------------------------------------------
-- Script       : 204_Create_DB_Objects_CruTS3_Pivot.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Transform to Formatted xyz table per Variable
-------------------------------------------------------------------------------
	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_cld;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_cld
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('cld' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_cld 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_cld TO PUBLIC;

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_pre;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_pre
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('pre' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_pre 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_pre TO PUBLIC;

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_wet;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_wet
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('wet' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_wet 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_wet TO PUBLIC;

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_dtr;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_dtr
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('dtr' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_dtr 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_dtr TO PUBLIC;

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_frs;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_frs
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('frs' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_frs 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_frs TO PUBLIC;


	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_tmn;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_tmn
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('tmn' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmn 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_tmn TO PUBLIC;


DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_tmp;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_tmp
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('vap' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmp 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_tmx;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_tmx
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('tmx' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmx 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_tmx TO PUBLIC;


	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_vap;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_vap
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '.' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
--      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
--      ,
      ,CASE 
		WHEN (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 > 9 THEN CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || 
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
		ELSE CAST((((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS VARCHAR) || '0'||
		     CAST((((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS VARCHAR)
	    END AS yyyymm
      ,CAST((CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS VARCHAR) AS xcoord
      ,CAST((CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS VARCHAR) AS ycoord
--      ,CAST('vap' AS VARCHAR) AS variable       
      ,token AS val
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_vap 
--                         LIMIT 1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_idx_vap TO PUBLIC;

--earth.cru_ts3_xyz_idx_t5
select * from  earth.cru_ts3_xyz limit 5;
DROP TABLE IF EXISTS earth.cru_ts3_xyz;
CREATE FACT TABLE earth.cru_ts3_xyz
DISTRIBUTE BY HASH (idx)
AS
SELECT pre.idx AS idx
      ,pre.yyyymm
      ,pre.xcoord
      ,pre.ycoord
      ,pre.val AS pre
      ,wet.val AS wet
      ,cld.val AS cld
      ,dtr.val AS dtr
      ,frs.val AS frs
      ,tmn.val AS tmn
      ,tmp.val AS tmp
      ,tmx.val AS tmx
      ,vap.val AS vap
FROM earth.cru_ts3_xyz_idx_pre pre
INNER JOIN earth.cru_ts3_xyz_idx_wet wet
ON pre.idx=wet.idx
INNER JOIN earth.cru_ts3_xyz_idx_cld cld
ON pre.idx=cld.idx
INNER JOIN earth.cru_ts3_xyz_idx_dtr dtr
ON pre.idx=dtr.idx
INNER JOIN earth.cru_ts3_xyz_idx_frs frs
ON pre.idx=frs.idx
INNER JOIN earth.cru_ts3_xyz_idx_tmn tmn
ON pre.idx=tmn.idx
INNER JOIN earth.cru_ts3_xyz_idx_tmp tmp
ON pre.idx=tmp.idx
INNER JOIN earth.cru_ts3_xyz_idx_tmx tmx
ON pre.idx=tmx.idx
INNER JOIN earth.cru_ts3_xyz_idx_vap vap
ON pre.idx=vap.idx;
GRANT SELECT ON TABLE earth.cru_ts3_xyz TO PUBLIC;

SELECT * FROM earth.cru_ts3_xyz_idx_pre where val not ilike '%-999%' limit 5;
SELECT * FROM earth.cru_ts3_xyz where pre not ilike '%-999%' limit 5;

SELECT COUNT(*) FROM earth.cru_ts3_xyz LIMIT 5;
SELECT COUNT(*) FROM earth.cru_ts3_xyz_idx_vap LIMIT 5;
SELECT COUNT(*) FROM earth.cru_ts3_vap LIMIT 5;

select * from earth.cru_ts3_xyz_tmp limit 10
--'pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap'

SELECT * FROM earth.cru_ts3_xyz where pre ilike '%.%' 
or wet ilike '%.%'
or
cld ilike '%.%'
or
dtr ilike '%.%'
or
frs ilike '%.%'
or
tmn ilike '%.%'
or
tmp ilike '%.%'
or
tmx ilike '%.%'
or
vap ilike '%.%'
limit 100;
ALTER TABLE earth.cru_ts3_xyz RENAME TO cru_ts3_xyz_tmp;
ALTER TABLE earth.cru_ts3_xyz_temp RENAME TO cru_ts3_xyz;
GRANT SELECT ON TABLE earth.cru_ts3_xyz TO PUBLIC;
SELECT COUNT(*) FROM earth.cru_ts3_xyz;
SELECT COUNT(*) FROM earth.cru_ts3_xyz_tmp;

DROP TABLE IF EXISTS earth.cru_ts3_xyz_temp;
CREATE FACT TABLE earth.cru_ts3_xyz_temp
DISTRIBUTE BY HASH (idx)
AS
SELECT 
       idx
      ,CAST(yyyymm AS INTEGER) AS yyyymm
      ,CAST(xcoord AS DOUBLE) AS xcoord
      ,CAST(ycoord AS DOUBLE) AS ycoord
      ,CAST(pre AS INTEGER) AS pre
      ,CAST(wet AS INTEGER) AS wet
      ,CAST(cld AS INTEGER) AS cld
      ,CAST(dtr AS INTEGER) AS dtr
      ,CAST(frs AS INTEGER) AS frs
      ,CAST(tmn AS INTEGER) AS tmn
      ,CAST(tmp AS INTEGER) AS tmp
      ,CAST(tmx AS INTEGER) AS tmx
      ,CAST(vap AS INTEGER) AS vap
      FROM earth.cru_ts3_xyz;
