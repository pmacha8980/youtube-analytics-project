-- YouTube Analytics Project - Create Enhanced Analytics Views

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE YOUTUBE_ANALYTICS_WH;

-- Create a regular view for daily metrics
CREATE OR REPLACE VIEW ANALYTICS.DAILY_VIDEO_METRICS AS
SELECT
  DATE_TRUNC('DAY', v.publish_date) AS day,  -- Changed from publish_time to publish_date
  c.category_name,
  COUNT(DISTINCT v.video_id) AS video_count,
  SUM(f.views) AS total_views,
  SUM(f.likes) AS total_likes,
  SUM(f.dislikes) AS total_dislikes,
  SUM(f.comment_count) AS total_comments,
  AVG(f.views) AS avg_views,
  AVG(f.likes) AS avg_likes,
  AVG(f.dislikes) AS avg_dislikes
FROM DWH.FACT_VIDEO_STATS f
JOIN DWH.DIM_VIDEO v ON f.video_key = v.video_key
JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
WHERE v.is_current = TRUE
GROUP BY 1, 2;

-- Create a materialized view on a single table for demonstration
CREATE OR REPLACE MATERIALIZED VIEW ANALYTICS.CATEGORY_METRICS AS
SELECT
  category_id,
  category_name,
  COUNT(*) AS video_count,
  MIN(load_timestamp) AS first_loaded,
  MAX(load_timestamp) AS last_loaded
FROM DWH.DIM_CATEGORY
GROUP BY 1, 2;

-- Log the creation
CALL RAW.LOG_PIPELINE_STEP(
  'ANALYTICS', 
  'CREATE_ENHANCED_ANALYTICS_VIEWS', 
  'SUCCESS', 
  2, 
  NULL
);
