-------------------------------------------------------------------------------
-- Script       : 100_Create_DB_Objects.sql
-- @description : Create the tables for data ingestion.
--                
-- Author(s)    : BIG DATA MNL
-- History      : 20161020 Timothy Santos Created
--
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Create table for ELT of place names data
-- Raw file has no header
--
--The main 'geoname' table has the following fields :
-----------------------------------------------------
--geonameid         : integer id of record in geonames database
--name              : name of geographical point (utf8) varchar(200)
--asciiname         : name of geographical point in plain ascii characters, varchar(200)
--alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, 
--					convenience attribute from alternatename table, varchar(10000)
--latitude          : latitude in decimal degrees (wgs84)
--longitude         : longitude in decimal degrees (wgs84)
--feature class     : see http://www.geonames.org/export/codes.html, char(1)
--feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
--country code      : ISO-3166 2-letter country code, 2 characters
--cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
--admin1 code       : fipscode (subject to change to iso code), see exceptions below, 
--					see file admin1Codes.txt for display names of this code; varchar(20)
--admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
--admin3 code       : code for third level administrative division, varchar(20)
--admin4 code       : code for fourth level administrative division, varchar(20)
--population        : bigint (8 byte int) 
--elevation         : in meters, integer
--dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' 
--					(ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
--timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
--modification date : date of last modification in yyyy-MM-dd format

DROP TABLE IF EXISTS earth.all_countries_raw;
CREATE FACT TABLE earth.all_countries_raw
(  geonameid VARCHAR 
  ,name VARCHAR
  ,asciiname VARCHAR
  ,alternatenames VARCHAR
  ,latitude VARCHAR
  ,longitude VARCHAR
  ,feature_class VARCHAR
  ,feature_code VARCHAR
  ,country_code VARCHAR
  ,cc2 VARCHAR
  ,admin1_code VARCHAR
  ,admin2_code VARCHAR
  ,admin3_code VARCHAR
  ,admin4_code VARCHAR
  ,population VARCHAR
  ,elevation VARCHAR
  ,dem VARCHAR
  ,timezone VARCHAR
  ,modification_date VARCHAR
)
DISTRIBUTE BY HASH(geonameid);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors  earth.all_countries_raw  allCountries.txt &

-- 11134126 rows
GRANT SELECT ON TABLE earth.allcountries_raw TO public;
--
--cities1000.zip           : all cities with a population > 1000 or seats of adm div (ca 150.000), see 'geoname' table for columns
--cities5000.zip           : all cities with a population > 5000 or PPLA (ca 50.000), see 'geoname' table for columns
--cities15000.zip          : all cities with a population > 15000 or capitals (ca 25.000), see 'geoname' table for columns
ALTER TABLE  earth.all_countries_raw RENAME TO all_countries;
GRANT SELECT ON TABLE earth.all_countries TO public;

SELECT * FROM earth.all_countries LIMIT 100;

SELECT COUNT(*) FROM earth.all_countries;
SELECT COUNT(DISTINCT geonameid) FROM earth.all_countries;
SELECT COUNT(*) FROM earth.all_cities;
SELECT COUNT(DISTINCT geonameid) FROM earth.all_cities;

SELECT COUNT(DISTINCT b.geonameid) FROM (SELECT * FROM earth.all_countries d UNION SELECT * FROM earth.all_cities c ) b ;


-------------------------------------------------------------------------------
-- Create table for ELT of place names data
-- Raw file has no header
--The table 'alternate names' :
-------------------------------
--alternateNameId   : the id of this alternate name, int
--geonameid         : geonameId referring to id in table 'geoname', int
--isolanguage       : iso 639 language code 2- or 3-characters; 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link for a website, varchar(7)
--alternate name    : alternate name or name variant, varchar(400)
--isPreferredName   : '1', if this alternate name is an official/preferred name
--isShortName       : '1', if this is a short name like 'California' for 'State of California'
--isColloquial      : '1', if this alternate name is a colloquial or slang term
--isHistoric        : '1', if this alternate name is historic and was used in the past
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS earth.alternate_names_raw;

CREATE DIMENSION TABLE earth.alternate_names_raw
(  alternateNameId VARCHAR
  ,geonameid VARCHAR
  ,isolanguage VARCHAR
  ,alternate_name VARCHAR
  ,isPreferredName VARCHAR
  ,isShortName VARCHAR
  ,isColloquial VARCHAR
  ,isHistoric VARCHAR
);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors earth.alternate_names_raw alternateNames.txt &
-- 11314978 - ok


ALTER TABLE  earth.alternate_names_raw RENAME TO alternate_names;
GRANT SELECT ON TABLE earth.alternate_names TO public;
SELECT * FROM earth.alternate_names LIMIT 5;

-------------------------------------------------------------------------------
-- Create table for ELT of cities place names data
-- Raw file has no header
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS earth.cities_1000;
CREATE FACT TABLE earth.cities_1000
(  geonameid VARCHAR 
  ,name VARCHAR
  ,asciiname VARCHAR
  ,alternatenames VARCHAR
  ,latitude VARCHAR
  ,longitude VARCHAR
  ,feature_class VARCHAR
  ,feature_code VARCHAR
  ,country_code VARCHAR
  ,cc2 VARCHAR
  ,admin1_code VARCHAR
  ,admin2_code VARCHAR
  ,admin3_code VARCHAR
  ,admin4_code VARCHAR
  ,population VARCHAR
  ,elevation VARCHAR
  ,dem VARCHAR
  ,timezone VARCHAR
  ,modification_date VARCHAR
)
DISTRIBUTE BY HASH(geonameid);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors  earth.cities_1000  cities1000.txt &

GRANT SELECT ON TABLE earth.cities_1000 TO public;


-------------------------------------------------------------------------------
-- Create table for ELT of cities place names data
-- Raw file has no header
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS earth.cities_5000;
CREATE FACT TABLE earth.cities_5000
(  geonameid VARCHAR 
  ,name VARCHAR
  ,asciiname VARCHAR
  ,alternatenames VARCHAR
  ,latitude VARCHAR
  ,longitude VARCHAR
  ,feature_class VARCHAR
  ,feature_code VARCHAR
  ,country_code VARCHAR
  ,cc2 VARCHAR
  ,admin1_code VARCHAR
  ,admin2_code VARCHAR
  ,admin3_code VARCHAR
  ,admin4_code VARCHAR
  ,population VARCHAR
  ,elevation VARCHAR
  ,dem VARCHAR
  ,timezone VARCHAR
  ,modification_date VARCHAR
)
DISTRIBUTE BY HASH(geonameid);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors  earth.cities_5000  cities5000.txt &

GRANT SELECT ON TABLE earth.cities_5000 TO public;


-------------------------------------------------------------------------------
-- Create table for ELT of cities place names data
-- Raw file has no header
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS earth.cities_15000;
CREATE FACT TABLE earth.cities_15000
(  geonameid VARCHAR 
  ,name VARCHAR
  ,asciiname VARCHAR
  ,alternatenames VARCHAR
  ,latitude VARCHAR
  ,longitude VARCHAR
  ,feature_class VARCHAR
  ,feature_code VARCHAR
  ,country_code VARCHAR
  ,cc2 VARCHAR
  ,admin1_code VARCHAR
  ,admin2_code VARCHAR
  ,admin3_code VARCHAR
  ,admin4_code VARCHAR
  ,population VARCHAR
  ,elevation VARCHAR
  ,dem VARCHAR
  ,timezone VARCHAR
  ,modification_date VARCHAR
)
DISTRIBUTE BY HASH(geonameid);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --el-enabled --el-discard-errors  earth.cities_15000  cities15000.txt &

GRANT SELECT ON TABLE earth.cities_15000 TO public;

--Country info
--#ISO	ISO3	ISO-Numeric	fips	Country	Capital	Area(in sq km)	Population	
--Continent	tld	CurrencyCode	CurrencyName	Phone	Postal Code Format	
--Postal Code Regex	Languages	geonameid	neighbours	EquivalentFipsCode

-------------------------------------------------------------------------------
-- Create table for ELT of cities place names data
-- Raw file has no header
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS earth.country_info;
CREATE FACT TABLE earth.country_info
(  ISO VARCHAR 
  ,ISO3 VARCHAR
  ,ISO_numeric VARCHAR
  ,fips VARCHAR
  ,Country VARCHAR
  ,Capital VARCHAR
  ,Area VARCHAR
  ,Population VARCHAR
  ,Continent VARCHAR
  ,tld VARCHAR
  ,CurrencyCode VARCHAR
  ,CurrencyName VARCHAR
  ,Phone VARCHAR
  ,postal_code_format VARCHAR
  ,postal_code_regex VARCHAR
  ,languages VARCHAR
  ,geonameid VARCHAR
  ,neighbours VARCHAR
  ,EquivalentFipsCode VARCHAR
)
DISTRIBUTE BY HASH(ISO);

nohup ncluster_loader -h 10.128.24.10 -U ts186045 -w ts186045 -d beehive --skip-rows 51 --el-enabled --el-discard-errors  earth.country_info  countryInfo.txt &

GRANT SELECT ON TABLE earth.country_info TO public;

SELECT * FROM earth.country_info LIMIT 5;



DROP TABLE IF EXISTS earth.all_cities;
CREATE FACT TABLE earth.all_cities
DISTRIBUTE BY HASH(geonameid)
AS
SELECT * FROM
earth.cities_1000
UNION SELECT * FROM
earth.cities_5000
UNION SELECT * FROM
earth.cities_15000
;

SELECT * FROM earth.all_cities LIMIT 5;
GRANT SELECT ON TABLE earth.all_cities TO public;



ALTER TABLE earth.all_cities SET SCHEMA stg;
DROP TABLE IF EXISTS earth.all_cities;
CREATE FACT TABLE earth.all_cities
DISTRIBUTE BY HASH (geonameid)
AS
SELECT
   CAST(geonameid AS INTEGER) AS geonameid 
  ,name 
  ,asciiname 
  ,alternatenames 
  ,CAST(latitude AS DOUBLE) AS latitude
  ,CAST(longitude AS DOUBLE) AS longitude
  ,feature_class 
  ,feature_code 
  ,country_code 
  ,cc2 
  ,admin1_code 
  ,admin2_code 
  ,admin3_code 
  ,admin4_code 
  ,CAST(population AS BIGINT) AS population
  ,elevation
  ,CAST(dem AS INTEGER) AS dem
  ,timezone 
  ,CAST(REGEXP_REPLACE(modification_date,'-','','g')  AS INTEGER) AS modification_date_yyyymmdd
FROM 
--earth.all_cities 
stg.all_cities
--LIMIT 5
;
GRANT SELECT ON TABLE earth.all_cities TO PUBLIC;

SELECT COUNT(*) FROM stg.all_cities;
SELECT COUNT(*) FROM earth.all_cities;




ALTER TABLE earth.all_countries SET SCHEMA stg;
DROP TABLE IF EXISTS earth.all_countries;
CREATE FACT TABLE earth.all_countries
DISTRIBUTE BY HASH (geonameid)
AS
SELECT
   CAST(geonameid AS INTEGER) AS geonameid 
  ,name 
  ,asciiname 
  ,alternatenames 
  ,CAST(latitude AS DOUBLE) AS latitude
  ,CAST(longitude AS DOUBLE) AS longitude
  ,feature_class 
  ,feature_code 
  ,country_code 
  ,cc2 
  ,admin1_code 
  ,admin2_code 
  ,admin3_code 
  ,admin4_code 
  ,CAST(population AS BIGINT) AS population
  ,elevation
  ,CAST(dem AS INTEGER) AS dem
  ,timezone 
  ,CAST(REGEXP_REPLACE(modification_date,'-','','g')  AS INTEGER) AS modification_date_yyyymmdd
FROM 
--earth.all_countries 
stg.all_countries
--LIMIT 5
;
GRANT SELECT ON TABLE earth.all_countries TO PUBLIC;

SELECT COUNT(*) FROM stg.all_countries;
SELECT COUNT(*) FROM earth.all_countries;

SELECT * FROM earth.all_cities LIMIT 10;

SELECT * FROM life.cambodia_monthly_cases_1993_to_2009 LIMIT 100;

SELECT admin_level1_area FROM life.cambodia_monthly_cases_1993_to_2009 LIMIT 100;

SELECT * FROM earth.alternate_names;

SELECT COUNT(*) FROM earth.alternate_names;

SELECT * FROM
earth.alternate_names a
INNER JOIN
life.cambodia_monthly_cases_1993_to_2009 b
ON a.alternate_name=b.admin_level1_area


SELECT * FROM
earth.alternate_names
,life.cambodia_monthly_cases_1993_to_2009
WHERE admin_level1_area ILIKE alternate_name

DROP TABLE IF EXISTS stg.leve_alternate_cambodia;
CREATE FACT TABLE stg.lev_alternate_cambodia
DISTRIBUTE BY HASH(geonameid)
AS
SELECT *
FROM ldist
(
ON (SELECT * FROM
earth.alternate_names
,life.cambodia_monthly_cases_1993_to_2009)
SOURCE('alternate_name')
TARGET('admin_level1_area')
ACCUMULATE('geonameid','yyyymm','cases')
)
WHERE distance<3;

SELECT COUNT(*) FROM stg.lev_alternate_cambodia;

SELECT DISTINCT target,source,distance FROM stg.lev_alternate_cambodia;

SELECT * FROM earth.all_cities where country_code='KH'
;

SELECT * FROM earth.all_cities LIMIT 100;
SELECT * FROM earth.ALTERNATE_NAMES LIMIT 100;

SELECT * FROM (SELECT DISTINCT regexp_replace(admin_level1_area,'[^A-Za-z]','','g') admin_lev1, admin_level1_area FROM life.cambodia_monthly_cases_1993_to_2009 ) a
INNER JOIN
(SELECT *, regexp_replace(alternate_name,'[^A-Za-z]','','g') AS  alternate_name_parsed FROM earth.alternate_names) b
ON a.admin_lev1=b.alternate_name_parsed

SELECT * FROM earth.all_cities where country_code='KH' LIMIT 100


SELECT * FROM 
(SELECT *,regexp_replace(token,'[^A-Za-z]','','g') AS  alternate_name_parsed 
 FROM text_parser(
 ON (SELECT * FROM earth.all_cities where country_code='KH')
 TEXT_COLUMN('alternatenames')
 CASE_INSENSITIVE('false')
 DELIMITER(',')
 ACCUMULATE('geonameid','name','asciiname','admin1_code')
 ) 
)a
INNER JOIN
(SELECT DISTINCT regexp_replace(admin_level1_area,'[^A-Za-z]','','g') admin_lev1, admin_level1_area FROM life.cambodia_monthly_cases_1993_to_2009 ) b
ON a.alternate_name_parsed=b.admin_lev1


SELECT * FROM 
(SELECT *,regexp_replace(token,'[^A-Za-z]','','g') AS  alternate_name_parsed 
 FROM text_parser(
 ON (SELECT * FROM earth.all_cities where country_code='KH')
 TEXT_COLUMN('alternatenames')
 CASE_INSENSITIVE('true')
 DELIMITER(',')
 ACCUMULATE('geonameid','name','asciiname','admin1_code')
 ) 
)a
INNER JOIN
(SELECT DISTINCT regexp_replace(admin_level1_area,'[^A-Za-z]','','g') admin_lev1, admin_level1_area FROM life.cambodia_monthly_cases_1993_to_2009 ) b
ON a.token=b.admin_level1_area



SELECT * FROM 
(SELECT *, regexp_replace(alternate_name,'[^A-Za-z]','','g') AS  alternate_name_parsed FROM earth.alternate_names
)a
INNER JOIN
(SELECT DISTINCT regexp_replace(admin_level1_area,'[^A-Za-z]','','g') admin_lev1, admin_level1_area FROM life.cambodia_monthly_cases_1993_to_2009 ) b
ON a.alternate_name_parsed=b.admin_lev1


   1831797 Battambang    Battambang    29          batdâmbâng                   1        5 batdmbng              batdmbng           batdâmbâng
   1831173 Kampong Cham  Kampong Cham  02          kâmpóng cham                 1        5 kmpngcham             kmpngcham          kâmpóng cham
   1831142 Sihanoukville Sihanoukville 28          krong preah sihanouk         1        9 krongpreahsihanouk    krongpreahsihanouk krong preah sihanouk
   1831133 Kampong Speu  Kampong Speu  04          kâmpóng spœ                  1        4 kmpngsp               kmpngsp            kâmpóng sp
   1831125 Kampong Thom  Kampong Thom  05          kâmpóng thum                 1        5 kmpngthum             kmpngthum          kâmpóng thum
   1831112 Kampot        Kampot        21          kâmpôt                       1       10 kmpt                  kmpt               kâmpôt
   1830564 Kratié        Kratie        09          krâchéh                      1        3 krchh                 krchh              krâchéh
   1830467 Krong Kep     Krong Kep     26          krŏng kêb                    1        4 krngkb                krngkb             krŏng kêb
   1830205 Pailin        Pailin        30          krong pailin                 1        2 krongpailin           krongpailin        krong pailin
   1822768 Pursat        Pursat        12          poŭthĭsăt                    1        4 pothst                pothst             poŭthĭsăt
   1822610 Prey Veng     Prey Veng     14          prey vêng                    1        2 preyvng               preyvng            prey vêng
   1822029 Stung Treng   Stung Treng   17          stœ̆ng trêng                 1        4 stngtrng              stngtrng           stng trêng
   1821993 Svay Rieng    Svay Rieng    18          svay rieng                   1        6 svayrieng             svayrieng          svay rieng
   1821940 Takeo         Takeo         19          takêv                        1        5 takv                  takv               takêv
   1821306 Phnom Penh    Phnom Penh    22          phnom penh                   1        7 phnompenh             phnompenh          phnom penh
   1821306 Phnom Penh    Phnom Penh    22          phnom-penh                   1        9 phnompenh             phnompenh          phnom penh
   1821306 Phnom Penh    Phnom Penh    22          phnompenh                    1       11 phnompenh             phnompenh          phnom penh

      1831797 Battambang    Battambang    29          batdâmbâng                   1        5 batdmbng              batdmbng           batdâmbâng
   1831173 Kampong Cham  Kampong Cham  02          kâmpóng cham                 1        5 kmpngcham             kmpngcham          kâmpóng cham
   1831125 Kampong Thom  Kampong Thom  05          kâmpóng thum                 1        5 kmpngthum             kmpngthum          kâmpóng thum
   1831112 Kampot        Kampot        21          kâmpôt                       1       10 kmpt                  kmpt               kâmpôt
   1830564 Kratié        Kratie        09          krâchéh                      1        3 krchh                 krchh              krâchéh
   1830467 Krong Kep     Krong Kep     26          krŏng kêb                    1        4 krngkb                krngkb             krŏng kêb
   1830205 Pailin        Pailin        30          krong pailin                 1        2 krongpailin           krongpailin        krong pailin
   1821306 Phnom Penh    Phnom Penh    22          phnom penh                   1        7 phnompenh             phnompenh          phnom penh
   1822610 Prey Veng     Prey Veng     14          prey vêng                    1        2 preyvng               preyvng            prey vêng
   1822768 Pursat        Pursat        12          poŭthĭsăt                    1        4 pothst                pothst             poŭthĭsăt
   1831142 Sihanoukville Sihanoukville 28          krong preah sihanouk         1        9 krongpreahsihanouk    krongpreahsihanouk krong preah sihanouk
   1821993 Svay Rieng    Svay Rieng    18          svay rieng                   1        6 svayrieng             svayrieng          svay rieng
   1821940 Takeo         Takeo         19          takêv                        1        5 takv                  takv               takêv

   