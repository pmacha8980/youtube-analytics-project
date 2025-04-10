-- YouTube Analytics Project - Create Dimension Tables

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA DWH;
USE WAREHOUSE YOUTUBE_TRANSFORM_WH;


/*1. TABLE(GENERATOR(ROWCOUNT => 1000)) creates a virtual table with 1000 rows
2. SEQ4() generates a sequence of numbers for these rows
3. ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 assigns a sequential number starting from 0 to each row
4. This value is stored as seq
This technique is creating a date dimension table with:
• One row per day
• Starting from the earliest date in your data
• Extending for 1000 days
• Including various date attributes (year, month, day, day of week, etc.)*/

CREATE OR REPLACE TABLE DWH.DIM_DATE AS
WITH date_range AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
  FROM TABLE(GENERATOR(ROWCOUNT => 1000))
),
date_dimension AS (
  SELECT
    TO_CHAR(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS)), 'YYYYMMDD')::NUMBER AS date_key,
    DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS)) AS full_date,
    YEAR(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS year,
    MONTH(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS month,
    MONTHNAME(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS month_name,
    DAY(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS day,
    DAYOFWEEK(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS day_of_week,
    DAYNAME(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS day_name,
    QUARTER(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS quarter,
    WEEKOFYEAR(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) AS week_of_year,
    CASE 
      WHEN DAYOFWEEK(DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS))) IN (0, 6) THEN TRUE 
      ELSE FALSE 
    END AS is_weekend
  FROM date_range
  WHERE DATEADD(DAY, seq, (SELECT MIN(publish_date) FROM STAGING.VIDEOS)) <= CURRENT_DATE()
)
SELECT * FROM date_dimension;


-- Create category dimension table
CREATE OR REPLACE TABLE DWH.DIM_CATEGORY AS
SELECT
  category_id AS category_key,
  category_id,
  category_name,
  assignable,
  etag,
  source_file,
  load_timestamp,
  -- Add surrogate key for SCD handling if needed
  ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY load_timestamp) AS version,
  TRUE AS is_current
FROM STAGING.CATEGORIES;

-- Create video dimension table
CREATE OR REPLACE TABLE DWH.DIM_VIDEO AS
SELECT
  video_id AS video_key,
  video_id,
  title,
  channel_title,
  category_id,
  publish_date,
  publish_year,
  publish_month,
  publish_day,
  tags,
  thumbnail_link,
  comments_disabled,
  ratings_disabled,
  video_error_or_removed,
  description,
  source_file,
  load_timestamp,
  -- Add surrogate key for SCD handling if needed
  ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY load_timestamp) AS version,
  TRUE AS is_current
FROM STAGING.VIDEOS;

-- Log the transformation step
CALL RAW.LOG_PIPELINE_STEP(
  'DATA_TRANSFORMATION', 
  'CREATE_DIMENSION_TABLES', 
  'SUCCESS', 
  (SELECT COUNT(*) FROM DWH.DIM_VIDEO) + (SELECT COUNT(*) FROM DWH.DIM_CATEGORY) + (SELECT COUNT(*) FROM DWH.DIM_DATE), 
  NULL
);
