-- YouTube Analytics Project - Create Analytics Views

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA ANALYTICS;
USE WAREHOUSE YOUTUBE_ANALYTICS_WH;

-- View for top 10 viewed videos by category
CREATE OR REPLACE VIEW ANALYTICS.TOP_VIDEOS_BY_CATEGORY AS
WITH ranked_videos AS (
  SELECT
    c.category_name,
    v.title,
    v.channel_title,
    f.views,
    ROW_NUMBER() OVER (PARTITION BY c.category_name ORDER BY f.views DESC) AS rank
  FROM DWH.FACT_VIDEO_STATS f
  JOIN DWH.DIM_VIDEO v ON f.video_key = v.video_id
  JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
)
SELECT
  category_name,
  title,
  channel_title,
  views
FROM ranked_videos
WHERE rank <= 10;

-- View for 5 overall most disliked videos
CREATE OR REPLACE VIEW ANALYTICS.MOST_DISLIKED_VIDEOS AS
SELECT
  v.title,
  v.channel_title,
  c.category_name,
  f.dislikes,
  f.views,
  (f.dislikes / NULLIF(f.views, 0)) * 100 AS dislike_percentage
FROM DWH.FACT_VIDEO_STATS f
JOIN DWH.DIM_VIDEO v ON f.video_key = v.video_id
JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
WHERE f.dislikes IS NOT NULL
ORDER BY f.dislikes DESC
LIMIT 5;

-- View for average likes in 'Autos & Vehicles' category
CREATE OR REPLACE VIEW ANALYTICS.AUTO_CATEGORY_LIKES AS
SELECT
  c.category_name,
  AVG(f.likes) AS avg_likes,
  MIN(f.likes) AS min_likes,
  MAX(f.likes) AS max_likes,
  COUNT(DISTINCT f.video_key) AS video_count
FROM DWH.FACT_VIDEO_STATS f
JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
WHERE c.category_name = 'Autos & Vehicles'
GROUP BY c.category_name;

-- View for unique videos in top 3 categories by likes, published during 2014-2018
CREATE OR REPLACE VIEW ANALYTICS.TOP_CATEGORIES_VIDEOS_2014_2018 AS
WITH top_categories AS (
  SELECT
    c.category_id,
    c.category_name,
    SUM(f.likes) AS total_likes,
    ROW_NUMBER() OVER (ORDER BY SUM(f.likes) DESC) AS category_rank
  FROM DWH.FACT_VIDEO_STATS f
  JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
  JOIN DWH.DIM_VIDEO v ON f.video_key = v.video_id
  JOIN DWH.DIM_DATE d ON f.date_key = d.date_key
  WHERE d.year BETWEEN 2014 AND 2018
  GROUP BY c.category_id, c.category_name
)
SELECT
  tc.category_name,
  tc.total_likes,
  COUNT(DISTINCT v.video_id) AS unique_video_count,
  MIN(d.year) AS min_year,
  MAX(d.year) AS max_year
FROM top_categories tc
JOIN DWH.FACT_VIDEO_STATS f ON tc.category_id = f.category_key
JOIN DWH.DIM_VIDEO v ON f.video_key = v.video_id
JOIN DWH.DIM_DATE d ON f.date_key = d.date_key
WHERE tc.category_rank <= 3
  AND d.year BETWEEN 2014 AND 2018
GROUP BY tc.category_name, tc.total_likes
ORDER BY tc.total_likes DESC;

-- View for video counts by file name and load date
CREATE OR REPLACE VIEW ANALYTICS.VIDEO_COUNTS_BY_SOURCE AS
SELECT
  f.source_file,
  DATE(f.load_timestamp) AS load_date,
  COUNT(DISTINCT f.video_key) AS video_count
FROM DWH.FACT_VIDEO_STATS f
GROUP BY f.source_file, DATE(f.load_timestamp)
ORDER BY load_date, source_file;

-- Log the analytics views creation
CALL RAW.LOG_PIPELINE_STEP(
  'DATA_ANALYTICS', 
  'CREATE_ANALYTICS_VIEWS', 
  'SUCCESS', 
  5, -- Number of views created
  NULL
);
