-------------------------------------------------------------------------------
-- Script       : 200_Create_DB_Objects_epidemiology_brazil.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161020 Timothy Santos Created
--
-------------------------------------------------------------------------------


-- Create table for Albopictus aedes
--1. VECTOR: Identifying the species; Ae. aegypti or Ae. albopictus
--2. OCCURRENCE_ID: Unique identifier for each occurrence in the database after
--temporal and spatial standardisation.
--3. SOURCE_TYPE: Published literature or unpublished sources with reference ID
--that corresponds to the full list of references in the supplementary information.
--4. LOCATION_TYPE: Whether the record represents a point or a polygon location.
--5. POLYGON_ADMIN: Admin level or polygon size which the record represents
--when the location type is a polygon. -999 when the location type is a point (5 km x
--5 km).
--6. X: The longitudinal coordinate of the point or polygon centroid (WGS1984 Datum).
--7. Y: The latitudinal coordinate of the point or polygon centroid (WGS1984 Datum).
--8. YEAR: The year of the occurrence.
--9. COUNTRY: The name of the country within which the occurrence lies.
--10.COUNTRY_ID: ISO alpha-3 country codes.
--11.GAUL_AD0: The country-level global administrative unit layer (GAUL) code (see
--http://www.fao.org/geonetwork) which identifies the Admin-0 polygon within
--which any smaller polygons and points lie.
--12.STATUS: Established vs. transient populations

DROP TABLE IF EXISTS stg.aegypti_albopictus_kraemer2015;
CREATE DIMENSION TABLE stg.aegypti_albopictus_kraemer2015
(  data VARCHAR
);
nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 -C "data" stg.aegypti_albopictus_kraemer2015  aegypti_albopictusKraemer2015.csv &

SELECT * FROM stg.aegypti_albopictus_kraemer2015 LIMIT 5;


DROP TABLE IF EXISTS stg.aegypti_albopictus_kraemer2015_tmp;
CREATE FACT TABLE stg.aegypti_albopictus_kraemer2015_tmp 
DISTRIBUTE BY HASH (OCCURRENCE_ID)
AS
SELECT *
FROM UNPACK (
              ON (
                  select * from stg.aegypti_albopictus_kraemer2015
                 )
                                 
              DATA_COLUMN('data')
              COLUMN_NAMES('vector','OCCURRENCE_ID','SOURCE_TYPE','LOCATION_TYPE','POLYGON_ADMIN','x'
                            ,'y','YYYY','COUNTRY','COUNTRY_ID','GAUL_AD0'
                            ,'STATUS')
              COLUMN_TYPES ('varchar','INTEGER', 'varchar','varchar', 'varchar'
                           ,'varchar', 'varchar','INTEGER', 'varchar','varchar'
                           ,'INTEGER','varchar')
              COLUMN_DELIMITER(',')
              DATA_GROUP(1)
              IGNORE_BAD_ROWS('true')
            );
            
            GRANT SELECT ON TABLE stg.aegypti_albopictus_kraemer2015_tmp TO public;


DROP TABLE IF EXISTS life.aegypti_albopictus_kraemer2015;
CREATE FACT TABLE life.aegypti_albopictus_kraemer2015
DISTRIBUTE BY HASH(OCCURRENCE_ID)
AS
SELECT vector
      ,occurrence_id
      ,source_type
      ,location_type
      ,polygon_admin
      ,CAST(X AS DOUBLE) AS x
      ,CAST(Y AS DOUBLE) AS y
      ,yyyy
      ,country
      ,country_id
      ,gaul_ad0
      ,status
FROM stg.aegypti_albopictus_kraemer2015_tmp;      

SELECT COUNT(*) FROM life.aegypti_albopictus_kraemer2015;

DROP TABLE IF EXISTS life.aegypti_albopictus_kraemer2015;
CREATE FACT TABLE life.aegypti_albopictus_kraemer2015
(  vector VARCHAR 
  ,OCCURRENCE_ID INTEGER
  ,SOURCE_TYPE VARCHAR
  ,LOCATION_TYPE VARCHAR
  ,POLYGON_ADMIN VARCHAR
  ,X DOUBLE
  ,Y DOUBLE
  ,YYYY INTEGER
  ,COUNTRY VARCHAR
  ,COUNTRY_ID VARCHAR
  ,GAUL_AD0 INTEGER
  ,STATUS VARCHAR
)
DISTRIBUTE BY HASH(OCCURRENCE_ID);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors --skip-rows 1 -c life.aegypti_albopictus_kraemer2015  aegypti_albopictusKraemer2015.csv &

GRANT SELECT ON TABLE life.aegypti_albopictus_kraemer2015 TO public;

SELECT * FROM life.aegypti_albopictus_kraemer2015 limit 5;

