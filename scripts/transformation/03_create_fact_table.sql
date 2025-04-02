-- YouTube Analytics Project - Create Fact Table

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA DWH;
USE WAREHOUSE YOUTUBE_TRANSFORM_WH;

-- Create fact table for video statistics
CREATE OR REPLACE TABLE DWH.FACT_VIDEO_STATS AS
SELECT
  v.video_id AS video_key,
  v.category_id AS category_key,
  d.date_key,
  v.views,
  v.likes,
  v.dislikes,
  v.comment_count,
  v.trending_date,
  v.source_file,
  v.load_timestamp
FROM STAGING.VIDEOS v
JOIN DWH.DIM_DATE d ON v.publish_date = d.full_date;

-- Create indexes for better query performance
ALTER TABLE DWH.FACT_VIDEO_STATS CLUSTER BY (date_key, category_key);

-- Log the transformation step
CALL RAW.LOG_PIPELINE_STEP(
  'DATA_TRANSFORMATION', 
  'CREATE_FACT_TABLE', 
  'SUCCESS', 
  (SELECT COUNT(*) FROM DWH.FACT_VIDEO_STATS), 
  NULL
);
