-- YouTube Analytics Project - Create Materialized Views

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE YOUTUBE_ANALYTICS_WH;

-- Create a materialized view for daily metrics
CREATE OR REPLACE MATERIALIZED VIEW ANALYTICS.DAILY_VIDEO_METRICS AS
SELECT
  DATE_TRUNC('DAY', v.publish_time) AS day,
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

-- Log the creation
CALL RAW.LOG_PIPELINE_STEP(
  'ANALYTICS', 
  'CREATE_MATERIALIZED_VIEWS', 
  'SUCCESS', 
  1, 
  NULL
);
