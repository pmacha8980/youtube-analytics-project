-- YouTube Analytics Project - Create Staging Tables

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA STAGING;
USE WAREHOUSE YOUTUBE_TRANSFORM_WH;

-- Create staging table for videos with data cleaning
CREATE OR REPLACE TABLE STAGING.VIDEOS AS
SELECT
  video_id,
  trending_date,
  TRIM(title) AS title,
  TRIM(channel_title) AS channel_title,
  category_id,
  publish_time,
  TRIM(tags) AS tags,
  NULLIF(views, 0) AS views,
  NULLIF(likes, 0) AS likes,
  NULLIF(dislikes, 0) AS dislikes,
  NULLIF(comment_count, 0) AS comment_count,
  TRIM(thumbnail_link) AS thumbnail_link,
  comments_disabled,
  ratings_disabled,
  video_error_or_removed,
  TRIM(description) AS description,
  source_file,
  load_timestamp,
  -- Extract date components for easier analysis
  DATE(publish_time) AS publish_date,
  YEAR(publish_time) AS publish_year,
  MONTH(publish_time) AS publish_month,
  DAY(publish_time) AS publish_day
FROM RAW.VIDEOS;

-- Create staging table for categories by parsing JSON
CREATE OR REPLACE TABLE STAGING.CATEGORIES AS
WITH parsed_categories AS (
  SELECT
    raw_json:items AS items,
    source_file
  FROM RAW.CATEGORIES
),
flattened_categories AS (
  SELECT
    value:id::NUMBER AS category_id,
    value:snippet.title::VARCHAR AS category_name,
    value:snippet.assignable::BOOLEAN AS assignable,
    value:etag::VARCHAR AS etag,
    source_file
  FROM parsed_categories,
  LATERAL FLATTEN(input => items)
)
SELECT
  category_id,
  category_name,
  assignable,
  etag,
  source_file,
  CURRENT_TIMESTAMP() AS load_timestamp
FROM flattened_categories;

-- Create a view to validate category IDs
CREATE OR REPLACE VIEW STAGING.CATEGORY_VALIDATION AS
SELECT
  v.video_id,
  v.category_id AS video_category_id,
  c.category_id AS valid_category_id,
  c.category_name,
  CASE WHEN c.category_id IS NULL THEN 'INVALID' ELSE 'VALID' END AS validation_status
FROM STAGING.VIDEOS v
LEFT JOIN STAGING.CATEGORIES c ON v.category_id = c.category_id;

-- Log the transformation step
CALL RAW.LOG_PIPELINE_STEP(
  'DATA_TRANSFORMATION', 
  'CREATE_STAGING_TABLES', 
  'SUCCESS', 
  (SELECT COUNT(*) FROM STAGING.VIDEOS), 
  NULL
);
