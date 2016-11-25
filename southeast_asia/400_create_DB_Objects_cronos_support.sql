-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects_Malaysia_2014.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161125 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of Sri Lanka data from cronos team
-------------------------------------------------------------------------------
 
DROP TABLE IF EXISTS stg.allcities_cronos;

CREATE DIMENSION TABLE stg.allcities_cronos
(
   ID INTEGER
  ,City VARCHAR
  ,Latitude NUMERIC
  ,Longitude NUMERIC
  ,Continent_Province VARCHAR
  ,continent VARCHAR
  ,province VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.allcities_cronos allCities4.csv  -c --skip-rows 1

ALTER TABLE stg.cronos_allcities SET SCHEMA earth;
GRANT SELECT ON stg.cronos_allcities TO public;

DROP TABLE IF EXISTS earth.country_iso_codes;

CREATE DIMENSION TABLE earth.country_iso_codes
(
   ID INTEGER
  ,country VARCHAR
  ,iso_codes3 VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors earth.country_iso_codes countryiso.csv  -c --skip-rows 1
GRANT SELECT ON earth.country_iso_codes TO PUBLIC;



DROP TABLE IF EXISTS stg.srilanka_monthly_complete_cronos;
CREATE DIMENSION TABLE stg.srilanka_monthly_complete_cronos
(
   ID NUMERIC
  ,admin_lvl1_capital_Longitude_adj NUMERIC
  ,admin_lvl1_capital_Latitude_adj NUMERIC
  ,year NUMERIC
  ,month NUMERIC
  ,admin_lvl1_name VARCHAR 
  ,country_iso2_id VARCHAR 
  ,country_name VARCHAR
  ,admin_lvl1_id VARCHAR
  ,period_date VARCHAR
  ,dengue_total_cases NUMERIC
  ,admin_lvl1_capital_Longitude NUMERIC
  ,admin_lvl1_capital_Latitude NUMERIC
  ,admin_lvl1_capital VARCHAR
  ,District VARCHAR
  ,LandArea_km2 NUMERIC
  ,InlandWaterArea_km2 NUMERIC
  ,TotalArea_km2 NUMERIC
  ,Population_totalArea NUMERIC
  ,TotalPopulation NUMERIC
  ,TotalPopulation_perc NUMERIC
  ,PopulationDensity_perkm2_District NUMERIC
  ,LandArea_km2_Province NUMERIC
  ,Population_totalArea_Province NUMERIC
  ,PopulationDensity_perkm2_Province NUMERIC
  ,dengueCases_perYear NUMERIC
  ,dengueCases_perMonth_percentage NUMERIC 
  ,dengueCases_totalYears NUMERIC
  ,degnueCases_totalYears_percentage NUMERIC
  ,idx NUMERIC
  ,yyyymm NUMERIC
  ,pre NUMERIC
  ,wet NUMERIC
  ,cld NUMERIC
  ,dtr NUMERIC
  ,frs NUMERIC
  ,tmn NUMERIC
  ,tmp NUMERIC
  ,tmx NUMERIC
  ,vap NUMERIC
  ,wet_averagePeYear NUMERIC
  ,cld_averagePerYear NUMERIC
  ,dtr_averagePerYear NUMERIC
  ,frs_averagePerYear NUMERIC
  ,tmn_averagePerYear NUMERIC
  ,tmp_averagePerYear NUMERIC
  ,tmx_averagePerYear NUMERIC
  ,vap_averageperYear NUMERIC
  ,VECTOR VARCHAR
  ,vector_coded VARCHAR
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.srilanka_monthly_complete_cronos sriLanka_monthly_complete2.csv  -c --skip-rows 1

ALTER TABLE stg.srilanka_monthly_complete_cronos SET SCHEMA life;
GRANT SELECT ON life.srilanka_monthly_complete_cronos TO public;


DROP TABLE IF EXISTS stg.srilanka_monthly_complete_2010_cronos;
CREATE DIMENSION TABLE stg.srilanka_monthly_complete_2010_cronos
(
  ID INTEGER
  ,year INTEGER
  ,month_numeric INTEGER
  ,Month VARCHAR
  ,Week_Ending VARCHAR
  ,Week INTEGER
  ,Cases_wholeCountryPerYear INTEGER
  ,idx NUMERIC
  ,yyyymm  INTEGER
  ,xcoord NUMERIC
  ,ycoord NUMERIC
  ,pre INTEGER
  ,wet INTEGER
  ,cld INTEGER
  ,dtr INTEGER
  ,frs INTEGER
  ,tmn INTEGER
  ,tmp INTEGER
  ,tmx INTEGER
  ,vap INTEGER
  ,pre_averagePerYear DOUBLE
  ,wet_averagePeYear DOUBLE
  ,cld_averagePerYear DOUBLE
  ,dtr_averagePerYear DOUBLE
  ,frs_averagePerYear DOUBLE
  ,tmn_averagePerYear DOUBLE
  ,tmp_averagePerYear DOUBLE
  ,tmx_averagePerYear DOUBLE
  ,vap_averageperYear DOUBLE
);

ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors stg.srilanka_monthly_complete_2010_cronos sriLanka_weekly2010_complete.csv  -c --skip-rows 1

ALTER TABLE stg.srilanka_monthly_complete_2010_cronos SET SCHEMA life;
GRANT SELECT ON life.srilanka_monthly_complete_2010_cronos TO public;

SELECT * FROM life.srilanka_monthly_complete_2010_cronos LIMIT 5;

