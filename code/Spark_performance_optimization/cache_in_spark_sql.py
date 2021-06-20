

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("./data/flight-data/csv/2015-summary.csv")

flightData2015.createOrReplaceTempView("flight_data_2015")

# without cache
spark.sql("""
WITH tab1 AS (
SELECT DEST_COUNTRY_NAME,
count(1) AS DEST_COUNT
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
),
tab2 AS (
SELECT ORIGIN_COUNTRY_NAME,
count(1) AS ORIGIN_COUNT
FROM flight_data_2015
GROUP BY ORIGIN_COUNTRY_NAME
)
SELECT
ORIGIN_COUNTRY_NAME AS COUNTRY,
ORIGIN_COUNT,
DEST_COUNT
FROM tab1
INNER JOIN tab2
ON DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME
""").show()

# with cache
spark.sql("CACHE TABLE c_flight_data_2015 SELECT * FROM flight_data_2015")
spark.sql("""
WITH tab1 AS (
SELECT DEST_COUNTRY_NAME,
count(1) AS DEST_COUNT
FROM c_flight_data_2015
GROUP BY DEST_COUNTRY_NAME
),
tab2 AS (
SELECT ORIGIN_COUNTRY_NAME,
count(1) AS ORIGIN_COUNT
FROM c_flight_data_2015
GROUP BY ORIGIN_COUNTRY_NAME
)
SELECT
ORIGIN_COUNTRY_NAME AS COUNTRY,
ORIGIN_COUNT,
DEST_COUNT
FROM tab1
INNER JOIN tab2
ON DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME
""").show()
spark.sql("CLEAR CACHE")
