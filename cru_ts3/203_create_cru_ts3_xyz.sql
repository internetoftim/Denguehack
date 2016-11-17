-------------------------------------------------------------------------------
-- Script       : 203_Create_cru_ts3_xyz.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161007 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Climate Research Unit data
-------------------------------------------------------------------------------
SELECT * FROM climate.gpwv4_longlat_xyz  limit 10
-------------------------------------------------------------------------------
-- Transform to Formatted xyz table
-------------------------------------------------------------------------------
--DROP TABLE IF EXISTS climate.gpwv4_longlat_xyztemp;
DROP TABLE IF EXISTS earth.cru_ts3_xyz_row_all;
CREATE DIMENSION TABLE earth.cru_ts3_xyz_row_all AS
SELECT * FROM climate.cru_ts3_xyz_row_all;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_row_all TO PUBLIC;

SELECT * FROM climate.cru_ts3_xyz_row_all LIMIT 5;
SELECT * FROM earth.cru_ts3_xyz_row_all LIMIT 5;

DROP TABLE IF EXISTS climate.cru_ts3_xyz_row_all;
CREATE FACT TABLE climate.cru_ts3_xyz_row_all
DISTRIBUTE BY HASH(asc_row)
AS
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('pre' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_pre 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
--SELECT * FROM climate.cru_ts3_xyz WHERE asc_row=4380 LIMIT 1000;
--SELECT * FROM climate.cru_ts3_xyz WHERE asc_col=719 LIMIT 1000;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('wet' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_wet 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('dtr' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_dtr 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('frs' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_frs 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('tmn' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmn 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('tmp' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmp 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('tmx' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_tmx 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('vap' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_vap 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

INSERT INTO climate.cru_ts3_xyz_row_all 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('cld' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_cld 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
GRANT SELECT ON TABLE climate.cru_ts3_xyz_row_all TO public;


DROP TABLE IF EXISTS climate.cru_ts3_xyz_row;
CREATE FACT TABLE climate.cru_ts3_xyz_row
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT 
    case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,xcoord
	,ycoord
	,variable
	,data AS val
FROM  climate.cru_ts3_xyz_row_all;

ALTER TABLE climate.cru_ts3_xyz_row RENAME TO earth.cru_ts3_xyz_row;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_row TO public;

DROP TABLE IF EXISTS earth.cru_ts3_xyz_row;
CREATE DIMENSION TABLE earth.cru_ts3_xyz_row AS
SELECT yyyymm
      ,CAST(xcoord AS VARCHAR)
      ,CAST(ycoord AS VARCHAR)
      ,variable
      ,val
FROM climate.cru_ts3_xyz_row;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_row TO PUBLIC;

SELECT COUNT (DISTINCT variable) FROM climate.cru_ts3_xyz_row LIMIT 10;

DROP TABLE IF EXISTS climate.cru_ts3_xyz_temprow;
CREATE FACT TABLE climate.cru_ts3_xyz_temprow
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT 
    case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,xcoord
	,ycoord
	,variable
	,data AS val
FROM  climate.cru_ts3_xyz_row_all
WHERE YEAR=1901;

SELECT DISTINCT variable FROM climate.cruts3_xyz_row_all;

DROP TABLE IF EXISTS climate.cru_ts3_xyz_row_temp;
CREATE FACT TABLE climate.cru_ts3_xyz_row_temp
DISTRIBUTE BY HASH(asc_row)
AS
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('pre' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_pre 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


INSERT INTO climate.cru_ts3_xyz_row_temp 
SELECT (CAST(sn AS INTEGER) -1) AS asc_col
      ,(CAST(rowid AS INTEGER) - 1) AS asc_row
      ,((CAST(rowid AS INTEGER) - 1))/360 + 1 AS count_month
      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS n_month
--      ,(((CAST(rowid AS INTEGER) - 1)/360)%12) + 1 AS month
      ,CASE (((CAST(rowid AS INTEGER) - 1)/360)%12) + 1
		WHEN 1 THEN 'JAN'
		WHEN 2 THEN 'FEB'
		WHEN 3 THEN 'MAR'
		WHEN 4 THEN 'APR'
		WHEN 5 THEN 'MAY'
		WHEN 6 THEN 'JUN'
		WHEN 7 THEN 'JUL'
		WHEN 8 THEN 'AUG'
		WHEN 9 THEN 'SEP'
		WHEN 10 THEN 'OCT'
		WHEN 11 THEN 'NOV'
		WHEN 12 THEN 'DEC'
		ELSE ''
	    END AS month
      ,(((CAST(rowid AS INTEGER) - 1)/360)/12) + 1901 AS year
      ,(CAST(((CAST(sn AS INTEGER) -1)%720) AS DOUBLE) * 0.5) - 179.75 AS xcoord
      ,(CAST(((CAST(rowid AS INTEGER) -1)%360) AS DOUBLE) * 0.5) - 89.75 AS ycoord
--      ,(-180 + CAST((SN-1) + 0.5  AS DOUBLE) * 0.0083333333333334) AS xcoord 
--      ,(-59.999999999992 + CAST(17400 + (-(rowid - 7)  + 0.5)  AS DOUBLE) * 0.0083333333333334) AS ycoord
      ,CAST('cld' AS VARCHAR) AS variable       
      ,token AS data
FROM TextTokenizer (
                    ON (
                         SELECT * FROM climate.cru_ts3_cld 
--                         LIMIT 2
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;

SELECT * FROM climate.cru_ts3_xyz_temprow  LIMIT 10;

(SELECT xcoord,ycoord,variable,data FROM climate.cru_ts3_xyz_row_temp)

CREATE FACT TABLE climate.pivot_temp
DISTRIBUTE BY HASH (asc_row)
AS
SELECT * FROM ts186045.pivot (
ON  (SELECT month, year, xcoord,ycoord,variable,data FROM climate.cru_ts3_xyz_row_all)
PARTITION BY  month,year, xcoord, ycoord
Partitions ('month','year', 'xcoord', 'ycoord')
Pivot_Keys ('pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap')
Metrics ('data')
)

SELECT * FROM climate.cru_ts3_xyz_columnar order by xcoord,ycoord limit 10

DROP TABLE IF EXISTS climate.cru_ts3_xyz_pivottemp;
CREATE FACT TABLE climate.cru_ts3_xyz_pivottemp
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_temprow 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap')
Pivot_Column ('variable')
Metrics ('val')
);

SELECT * FROM climate.cru_ts3_xyz_pivot LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_pre_pivot LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_pre_pivot WHERE val_pre IS NULL LIMIT 5;
SELECT COUNT(*) FROM climate.cru_ts3_xyz_pre_pivot ;
climate.cru_ts3_cld
SELECT COUNT(*) FROM climate.cru_ts3_cld where DATA IS NULL ;
SELECT * FROM climate.cru_ts3_cld limit 5 ;
SELECT * FROM climate.cru_ts3_xyz_row WHERE variable='cld' AND xcoord=-179.75 AND ycoord=-89.2 And yyyymm='190301' ;
SELECT * FROM climate.cru_ts3_xyz_pre_pivot WHERE xcoord=-179.75 AND yyy;

SELECT * FROM climate.cru_ts3_xyz_pre_pivot WHERE val_pre IS NULL

SELECT * FROM climate.cru_ts3_xyz_pre_pivot WHERE pre IS NULL

DROP TABLE IF EXISTS climate.cru_ts3_xyz_pre_pivot2;
CREATE FACT TABLE climate.cru_ts3_xyz_pre_pivot2
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
ORDER BY yyyymm
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre')
Pivot_Column ('variable')
Metrics ('val')
);
SELECT * FROM climate.cru_ts3_xyz_row WHERE yyyymm='190209' and xcoord='-0.25' and ycoord='-1.25';
SELECT * FROM climate.cru_ts3_xyz_row_all LIMIT 4;

DROP TABLE IF EXISTS climate.cru_ts3_xyz_cld_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_cld_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('cld')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_wet_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_wet_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('wet')
Pivot_Column ('variable')
Metrics ('val')
);

select * from CLIMATE.cru_ts3_xyz_pre_pivot ORDER BY YYYYMM, XCOORD limit 100

DROP TABLE IF EXISTS earth.cru_ts3_xyz_pre_pivot2;
CREATE FACT TABLE earth.cru_ts3_xyz_pre_pivot2
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON ( 
SELECT yyyymm
FROM climate.cru_ts3_xyz_row) 
PARTITION BY  yyyymm, xcoord, ycoord
ORDER BY YYYYMM
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_pre_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_pre_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
ORDER BY YYYYMM
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_dtr_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_dtr_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('dtr')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_frs_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_frs_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('frs')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_tmn_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_tmn_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('tmn')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_tmp_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_tmp_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('tmp')
Pivot_Column ('variable')
Metrics ('val')
);


DROP TABLE IF EXISTS climate.cru_ts3_xyz_tmx_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_tmx_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('tmx')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS climate.cru_ts3_xyz_vap_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_vap_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('vap')
Pivot_Column ('variable')
Metrics ('val')
);
SELECT * FROM climate.cru_ts3_xyz_pivottemp LIMIT 5;

SELECT * FROM climate.cru_ts3_xyz_row WHERE yyyymm='190103' AND xcoord=-179.75 AND ycoord=-89.75;

DROP TABLE IF EXISTS climate.cru_ts3_xyz_pivot;
CREATE FACT TABLE climate.cru_ts3_xyz_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON climate.cru_ts3_xyz_row 
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap')
Pivot_Column ('variable')
Metrics ('val')
);

SELECT * FROM  climate.cru_ts3_xyz_columnar where xcoord=-179.75 and YCOORD=-86.75 limit 51
SELECT * FROM  climate.cru_ts3_xyz_columnar where xcoord=-179.75 and YCOORD=-86.75 limit 51
SELECT * FROM  climate.cru_ts3_xyz_row_all where xcoord=-179.75 and YCOORD=-86.75 limit 51
SELECT * FROM  climate.cru_ts3_xyz_temp  limit 51

ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_pre TO pre;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_wet TO wet;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_cld TO cld;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_dtr TO dtr;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_frs TO frs;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_tmn TO tmn;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_tmp TO tmp;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_tmx TO tmx;
ALTER TABLE climate.cru_ts3_xyz_columnar RENAME COLUMN data_vap TO vap;

ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_pre TO pre;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_wet TO wet;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_cld TO cld;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_dtr TO dtr;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_frs TO frs;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_tmn TO tmn;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_tmp TO tmp;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_tmx TO tmx;
ALTER TABLE climate.cru_ts3_xyz_row_all RENAME COLUMN data_vap TO vap;

SELECT * FROM  climate.cru_ts3_xyz_row_all WHERE data ilike '%null%' limit 5;
SELECT * FROM  climate.cru_ts3_xyz_row_all WHERE data not ilike '%-999%' limit 5;

SELECT * FROM climate.cru_ts3_xyz_vap_pivot LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_vap_pivot where VAL_VAP IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_pre_pivot where VAL_PRE IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_wet_pivot where VAL_WET IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_cld_pivot where VAL_CLD IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_dtr_pivot where VAL_DTR IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_frs_pivot where VAL_FRS IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_tmn_pivot where VAL_TMN IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_tmp_pivot where VAL_TMP IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_tmx_pivot where VAL_TMX IS NULL LIMIT 5;
SELECT * FROM climate.cru_ts3_xyz_vap_pivot where VAL_VAP IS NULL LIMIT 5;

SELECT * FROM climate.cru_ts3_xyz_row_all where YYYYMM ILIKE '%190104%' and xcoord ilike '%-179.75%' and ycoord ilike '%-89.75%';
SELECT * FROM climate.cru_ts3_xyz_pre_pivot where YYYYMM ILIKE '%190104%' and xcoord ilike=-179.75 and ycoord=-89.25;
SELECT * FROM climate.cru_ts3_xyz_row_all where year=1901 and n_month=4 and xcoord=-179.75 and ycoord=-89.75;
SELECT * FROM climate.cru_ts3_xyz_vap_pivot where yyyymm ilike '%190104%' and xcoord=-179.75 and ycoord=-89.75;


--SELECT * FROM climate.gpwv4_longlat_xyz WHERE pop not ilike '%-9999%' AND year='2000' LIMIT 100;

DROP TABLE IF EXISTS earth.cru_ts3_xyz_pre_pivot;
CREATE FACT TABLE earth.cru_ts3_xyz_pre_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON (SELECT yyyymm
      ,CAST(xcoord AS VARCHAR) AS xcoord
      ,CAST(ycoord AS VARCHAR) AS ycoord
      ,variable
      ,val
      FROM climate.cru_ts3_xyz_row
)
PARTITION BY  yyyymm, xcoord, ycoord
ORDER BY YYYYMM
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre')
Pivot_Column ('variable')
Metrics ('val')
);

DROP TABLE IF EXISTS earth.cru_ts3_xyz_pivot;
CREATE FACT TABLE earth.cru_ts3_xyz_pivot
DISTRIBUTE BY HASH (yyyymm)
AS
SELECT * FROM ts186045.pivot (
ON (SELECT yyyymm
      ,CAST(xcoord AS VARCHAR) AS xcoord
      ,CAST(ycoord AS VARCHAR) AS ycoord
      ,variable
      ,val
      FROM climate.cru_ts3_xyz_row
)
PARTITION BY  yyyymm, xcoord, ycoord
Partitions ('yyyymm', 'xcoord', 'ycoord')
Pivot_Keys ('pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap')
Pivot_Column ('variable')
Metrics ('val')
);
SELECT * FROM earth.cru_ts3_xyz_pivot limit 10;
GRANT SELECT ON TABLE earth.cru_ts3_xyz_pivot TO PUBLIC;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_pre TO pre;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_wet TO wet;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_cld TO cld;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_dtr TO dtr;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_frs TO frs;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_tmn TO tmn;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_tmp TO tmp;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_tmx TO tmx;
ALTER TABLE earth.cru_ts3_xyz_pivot RENAME COLUMN val_vap TO vap;

SELECT COUNT(*) FROM climate.cru_ts3_xyz_row_all GROUP BY variable;


SELECT COUNT(*) FROM climate.cru_ts3_xyz_row_all WHERE variable='cld';
SELECT COUNT(*) FROM climate.cru_ts3_xyz_row_all WHERE variable='cld' and data='-999';
SELECT COUNT(*) FROM earth.cru_ts3_xyz_pivot WHERE CLD IS NOT NULL;
SELECT COUNT(*) FROM earth.cru_ts3_xyz_pivot WHERE VAP IS NOT NULL;

SELECT * FROM earth.cru_ts3_xyz_idx limit 5;

DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx;
CREATE FACT TABLE earth.cru_ts3_xyz_idr
DISTRIBUTE BY HASH (idx)
AS
SELECT 
cast(asc_row as varchar) || '.' || cast(asc_col as varchar) AS idx 
   ,case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,cast(xcoord as varchar)
	,cast(ycoord as varchar)
	,variable
	,data AS val
	FROM climate.cru_ts3_xyz_row_all;
	
	
--,case (n_month<10 ) 
--	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)|| '_' || cast(asc_row as varchar) || '_' || cast(asc_col as varchar)
--	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR) || '_' ||  cast(asc_row as varchar) || '_' || cast(asc_col as varchar)
--	end AS idx,	

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t1;
CREATE FACT TABLE earth.cru_ts3_xyz_idr_t1
DISTRIBUTE BY HASH (idx)
AS
SELECT 
cast(asc_row as varchar) || '.' || cast(asc_col as varchar) AS idx 
   ,case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,cast(xcoord as varchar)
	,cast(ycoord as varchar)
	,variable
	,data AS val
	FROM climate.cru_ts3_xyz_row_all
	WHERE variable='pre'
	ORDER BY idx
	LIMIT 50;
	
	
	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t2;
CREATE FACT TABLE earth.cru_ts3_xyz_idr_t2
DISTRIBUTE BY HASH (idx)
AS
SELECT 
cast(asc_row as varchar) || '.' || cast(asc_col as varchar) AS idx 
   ,case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,cast(xcoord as varchar)
	,cast(ycoord as varchar)
	,variable
	,data AS val
	FROM climate.cru_ts3_xyz_row_all
	WHERE variable='wet'
	ORDER BY idx
	LIMIT 50;	
	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t3;
CREATE FACT TABLE earth.cru_ts3_xyz_idr_t3
DISTRIBUTE BY HASH (idx)
AS
SELECT 
cast(asc_row as varchar) || '.' || cast(asc_col as varchar) AS idx 
   ,case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,cast(xcoord as varchar)
	,cast(ycoord as varchar)
	,variable
	,data AS val
	FROM climate.cru_ts3_xyz_row_all
	WHERE variable='cld'
	ORDER BY idx
	LIMIT 5;	

	
DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t4;
CREATE FACT TABLE earth.cru_ts3_xyz_idr_t4
DISTRIBUTE BY HASH (idx)
AS
SELECT 
cast(asc_row as varchar) || '.' || cast(asc_col as varchar) AS idx 
   ,case (n_month<10 ) 
	when TRUE then CAST(year AS VARCHAR) || '0' || CAST(n_month AS VARCHAR)
	else     CAST(year AS VARCHAR) || CAST(n_month AS VARCHAR)
	end AS YYYYMM
	,cast(xcoord as varchar)
	,cast(ycoord as varchar)
	,variable
	,data AS val
	FROM climate.cru_ts3_xyz_row_all
	WHERE variable='tmn'
	ORDER BY idx
	LIMIT 5;		

	

DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t5;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_t5
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '_' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
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
                         SELECT * FROM climate.cru_ts3_pre 
                         WHERE rowid=1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;


DROP TABLE IF EXISTS earth.cru_ts3_xyz_idx_t6;
CREATE FACT TABLE earth.cru_ts3_xyz_idx_t6
DISTRIBUTE BY HASH(idx)
AS
SELECT 
        CAST((CAST(rowid AS INTEGER) - 1) AS VARCHAR)|| '_' || CAST((CAST(sn AS INTEGER) -1) AS VARCHAR) AS idx
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
                         SELECT * FROM climate.cru_ts3_frs 
                         WHERE rowid=1
                       )
                    PARTITION BY ANY
                    TextColumn ('data') 
                    OutputByWord ('true')
                    Accumulate ('rowid')
                   )
;
--earth.cru_ts3_xyz_idx_t5

SELECT A.idx
      ,A.yyyymm
      ,A.xcoord
      ,A.ycoord
      ,A.val AS A
      ,B.val AS B
      ,C.val AS C
      FROM earth.cru_ts3_xyz_idx_t5 A
INNER JOIN earth.cru_ts3_xyz_idx_t4 B
ON A.IDX=B.IDX
INNER JOIN earth.cru_ts3_xyz_idx_t6 c
ON b.idx=c.idx
LIMIT 5;

--'pre','wet','cld','dtr','frs','tmn','tmp','tmx','vap'


SELECT * FROM  earth.cru_ts3_xyz_idx_t6 LIMIT 5
SELECT * FROM earth.cru_ts3_xyz_idr a  inner join
earth.cru_ts3_xyz_idr_t4 b
on a.idx=b.idx
WHERE a.variable='tmn'