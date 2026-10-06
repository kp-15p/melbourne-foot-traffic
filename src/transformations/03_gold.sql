-- 1 · Daily totals per sensor, with point-in-time sensor attributes.
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_daily_sensor_counts
  COMMENT 'Daily pedestrian totals per sensor, joined to the sensor as it was that day.'
AS
SELECT
  f.location_id,
  d.sensor_description,
  f.sensing_date,
  f.day_of_week,
  f.is_weekend,
  sum(f.pedestrian_count)                 AS daily_count,
  count(*)                                AS hours_reported,
  24 - count(*)                           AS hours_missing,
  max(f.pedestrian_count)                 AS peak_hour_count,
  max_by(f.hourday, f.pedestrian_count)   AS peak_hour,
  d.latitude, d.longitude
FROM silver.silver_pedestrian_hourly f
-- Point-in-time join: the sensor AS IT WAS on the sensing date, not as it is now.
LEFT JOIN silver.silver_sensor_scd d
  ON  f.location_id    = d.location_id
  AND f.sensing_date  >= d.`__START_AT`
  AND f.sensing_date  <  coalesce(d.`__END_AT`, DATE'9999-12-31')
GROUP BY ALL;


-- 2 · The typical day shape. The actual analytical question.
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_hourly_profile
  COMMENT 'Mean, median and p95 foot traffic by sensor, weekday and hour.'
AS
SELECT
  f.location_id,
  c.sensor_description,
  f.day_of_week,
  f.hourday,
  count(*)                                    AS observations,
  round(avg(f.pedestrian_count), 1)           AS mean_count,
  percentile_approx(f.pedestrian_count, 0.5)  AS median_count,
  percentile_approx(f.pedestrian_count, 0.95) AS p95_count
FROM silver.silver_pedestrian_hourly f
JOIN silver.silver_sensor_current   c ON c.location_id = f.location_id
WHERE f.sensing_date >= current_date() - INTERVAL 365 DAYS
GROUP BY ALL
-- One month gives ~4 of each weekday. Raise this to 8+ after the backfill —
-- an average built from three observations isn't one.
HAVING count(*) >= 8;


CREATE OR REFRESH MATERIALIZED VIEW gold.gold_sensor_health
  COMMENT 'Per sensor per day: did we get what we should have? Outages, not silence.'
AS
WITH calendar AS (
  -- How many hours were in this Melbourne local day? 24 almost always,
  -- 23 when DST starts, 25 when it ends. Derived from the tz database,
  -- not hardcoded — so it stays correct when the rules change.
  SELECT DISTINCT
    sensing_date,
    CAST((unix_timestamp(to_utc_timestamp(
            CAST(sensing_date + INTERVAL 1 DAY AS TIMESTAMP), 'Australia/Melbourne'))
        - unix_timestamp(to_utc_timestamp(
            CAST(sensing_date AS TIMESTAMP), 'Australia/Melbourne'))
         ) / 3600 AS INT) AS expected_hours
  FROM silver.silver_pedestrian_hourly
),
expected AS (
  SELECT c.location_id, c.sensor_description, cal.sensing_date, cal.expected_hours
  FROM   silver.silver_sensor_current c
  CROSS JOIN calendar cal
  WHERE  c.is_active
    AND  cal.sensing_date >= c.installation_date
),
actual AS (
  SELECT location_id, sensing_date,
         count(*)                       AS hours_reported,
         count_if(pedestrian_count = 0) AS zero_hours,
         sum(pedestrian_count)          AS daily_count
  FROM   silver.silver_pedestrian_hourly
  GROUP BY ALL
)
SELECT
  e.location_id,
  e.sensor_description,
  e.sensing_date,
  e.expected_hours,
  coalesce(a.hours_reported, 0)                      AS hours_reported,
  e.expected_hours - coalesce(a.hours_reported, 0)   AS hours_missing,
  coalesce(a.zero_hours, 0)                          AS zero_hours,
  a.daily_count,
  -- DST is a property of the DATE, not a diagnosis of the SENSOR.
  -- Keeping it as its own column is the actual fix; folding it into
  -- health_status is what produced the wrong answer.
  e.expected_hours <> 24                             AS is_dst_day,
  CASE
    WHEN a.hours_reported IS NULL                      THEN 'no_data'
    WHEN a.hours_reported <  e.expected_hours - 4      THEN 'partial_outage'
    WHEN a.zero_hours     >  12                        THEN 'suspect_all_zero'
    WHEN a.hours_reported <  e.expected_hours          THEN 'incomplete'
    ELSE 'healthy'
  END                                                AS health_status
FROM      expected e
LEFT JOIN actual   a USING (location_id, sensing_date);


-- 4 · Citywide trend, with the context that stops it misleading.
CREATE OR REFRESH MATERIALIZED VIEW gold.gold_city_daily
  COMMENT 'Citywide daily foot traffic, 7-day average and year-on-year comparison.'
AS
WITH daily AS (
  SELECT sensing_date, day_of_week,
         sum(pedestrian_count)       AS city_count,
         count(DISTINCT location_id) AS sensors_reporting
  FROM   silver.silver_pedestrian_hourly
  GROUP BY ALL
)
SELECT
  sensing_date, day_of_week, city_count, sensors_reporting,
  -- Comparing a day with 99 live sensors to one with 60 is meaningless.
  -- Normalising is what makes the trend line usable.
  round(city_count / nullif(sensors_reporting, 0))            AS count_per_sensor,
  round(avg(city_count) OVER (
    ORDER BY sensing_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
                                                              AS avg_7_day,
  -- 364, not 365: compares Friday to Friday. All NULL until the backfill.
  lag(city_count, 364) OVER (ORDER BY sensing_date)           AS same_weekday_last_year
FROM daily;