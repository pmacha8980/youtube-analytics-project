-- YouTube Analytics Project - Setup Orchestration

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;

-- Create a task to monitor for new files in the stage
CREATE OR REPLACE TASK RAW.MONITOR_NEW_FILES
  WAREHOUSE = YOUTUBE_LOAD_WH
  SCHEDULE = 'USING CRON 0 */6 * * * America/Los_Angeles' -- Run every 6 hours
AS
BEGIN
  -- Insert a log entry
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('DATA_INGESTION', 'MONITOR_NEW_FILES', 'RUNNING', 0, NULL);
  
  -- Load new CSV files
  COPY INTO RAW.VIDEOS (
    video_id, trending_date, title, channel_title, category_id, 
    publish_time, tags, views, likes, dislikes, comment_count, 
    thumbnail_link, comments_disabled, ratings_disabled, 
    video_error_or_removed, description, source_file
  )
  FROM (
    SELECT 
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
      METADATA$FILENAME
    FROM @YOUTUBE_EXTERNAL_STAGE/
  )
  FILE_FORMAT = RAW.CSV_FORMAT
  PATTERN = '.*videos\.csv\.gz'
  ON_ERROR = 'CONTINUE'
  FORCE = FALSE;  -- Only load new files
  
  -- Set file format to JSON
  ALTER STAGE YOUTUBE_EXTERNAL_STAGE SET FILE_FORMAT = RAW.JSON_FORMAT;
  
  -- Load new JSON files
  COPY INTO RAW.CATEGORIES (raw_json, source_file)
  FROM (
    SELECT 
      $1,
      METADATA$FILENAME
    FROM @YOUTUBE_EXTERNAL_STAGE/
  )
  FILE_FORMAT = RAW.JSON_FORMAT
  PATTERN = '.*_category_id\.json\.gz'
  ON_ERROR = 'CONTINUE'
  FORCE = FALSE;  -- Only load new files
  END;
  -- Reset file format to CSV
  ALTER STAGE YOUTUBE_EXTERNAL_STAGE SET FILE_FORMAT = RAW.CSV_FORMAT;
  
  -- Insert a log entry for completion
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  SELECT 
    'DATA_INGESTION', 
    'MONITOR_NEW_FILES_COMPLETE', 
    'SUCCESS', 
    COUNT(*), 
    NULL
  FROM RAW.VIDEOS;

-- Create a stream to track changes in the raw videos table
CREATE OR REPLACE STREAM RAW.VIDEO_STREAM ON TABLE RAW.VIDEOS;

-- Create a stream to track changes in the raw categories table
CREATE OR REPLACE STREAM RAW.CATEGORY_STREAM ON TABLE RAW.CATEGORIES;

-- Create a task in the RAW schema to process new videos from raw to staging
CREATE OR REPLACE TASK RAW.PROCESS_NEW_VIDEOS
  WAREHOUSE = YOUTUBE_TRANSFORM_WH
  AFTER RAW.MONITOR_NEW_FILES
AS
MERGE INTO STAGING.VIDEOS t
USING (
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
    DATE(publish_time) AS publish_date,
    YEAR(publish_time) AS publish_year,
    MONTH(publish_time) AS publish_month,
    DAY(publish_time) AS publish_day
  FROM RAW.VIDEO_STREAM
) s
ON t.video_id = s.video_id AND t.trending_date = s.trending_date
WHEN MATCHED THEN
  UPDATE SET
    t.title = s.title,
    t.channel_title = s.channel_title,
    t.category_id = s.category_id,
    t.publish_time = s.publish_time,
    t.tags = s.tags,
    t.views = s.views,
    t.likes = s.likes,
    t.dislikes = s.dislikes,
    t.comment_count = s.comment_count,
    t.thumbnail_link = s.thumbnail_link,
    t.comments_disabled = s.comments_disabled,
    t.ratings_disabled = s.ratings_disabled,
    t.video_error_or_removed = s.video_error_or_removed,
    t.description = s.description,
    t.source_file = s.source_file,
    t.load_timestamp = s.load_timestamp,
    t.publish_date = s.publish_date,
    t.publish_year = s.publish_year,
    t.publish_month = s.publish_month,
    t.publish_day = s.publish_day
WHEN NOT MATCHED THEN
  INSERT (
    video_id, trending_date, title, channel_title, category_id, 
    publish_time, tags, views, likes, dislikes, comment_count, 
    thumbnail_link, comments_disabled, ratings_disabled, 
    video_error_or_removed, description, source_file, load_timestamp,
    publish_date, publish_year, publish_month, publish_day
  )
  VALUES (
    s.video_id, s.trending_date, s.title, s.channel_title, s.category_id, 
    s.publish_time, s.tags, s.views, s.likes, s.dislikes, s.comment_count, 
    s.thumbnail_link, s.comments_disabled, s.ratings_disabled, 
    s.video_error_or_removed, s.description, s.source_file, s.load_timestamp,
    s.publish_date, s.publish_year, s.publish_month, s.publish_day
  );

-- Create a task to process new categories from raw to staging
CREATE OR REPLACE TASK RAW.PROCESS_NEW_CATEGORIES
  WAREHOUSE = YOUTUBE_TRANSFORM_WH
  AFTER RAW.MONITOR_NEW_FILES
AS
MERGE INTO STAGING.CATEGORIES t
USING (
  SELECT
    c.value:id::INTEGER AS category_id,
    c.value:snippet.title::STRING AS category_name,
    c.value:snippet.assignable::BOOLEAN AS assignable,
    --c.value:snippet.channelId::STRING AS channel_id,
     c.value:etag::VARCHAR AS etag,
    cs.source_file,
   -- cs.load_timestamp
    CURRENT_TIMESTAMP() AS load_timestamp
  FROM RAW.CATEGORY_STREAM cs,
  LATERAL FLATTEN(input => PARSE_JSON(cs.raw_json):items) c
) s
ON t.category_id = s.category_id
WHEN MATCHED THEN
  UPDATE SET
    t.category_name = s.category_name,
    t.assignable = s.assignable,
    --t.channel_id = s.channel_id,
     t.etag = s.etag,
    t.source_file = s.source_file,
    t.load_timestamp = s.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    --category_id, category_name, assignable, channel_id, source_file, load_timestamp
    category_id, category_name, assignable, etag, source_file, load_timestamp
  )
  VALUES (
   -- s.category_id, s.category_name, s.assignable, s.channel_id, s.source_file, s.load_timestamp
    s.category_id, s.category_name, s.assignable, s.etag, s.source_file, s.load_timestamp
  );

-- Create a stream to track changes in the staging videos table
CREATE OR REPLACE STREAM RAW.STAGING_VIDEO_STREAM ON TABLE STAGING.VIDEOS;

-- Create a stream to track changes in the staging categories table
CREATE OR REPLACE STREAM RAW.STAGING_CATEGORY_STREAM ON TABLE STAGING.CATEGORIES;

-- Create a task to update dimension tables
CREATE OR REPLACE TASK RAW.UPDATE_DIMENSIONS
  WAREHOUSE = YOUTUBE_TRANSFORM_WH
  AFTER RAW.PROCESS_NEW_VIDEOS, RAW.PROCESS_NEW_CATEGORIES
AS
BEGIN
  -- Update video dimension
  MERGE INTO DWH.DIM_VIDEO t
  USING (
    SELECT
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
      load_timestamp
    FROM RAW.STAGING_VIDEO_STREAM
  ) s
  ON t.video_id = s.video_id AND t.is_current = TRUE
  WHEN MATCHED AND (
    t.title != s.title OR
    t.channel_title != s.channel_title OR
    t.category_id != s.category_id OR
    t.description != s.description
  ) THEN
    UPDATE SET is_current = FALSE
  WHEN NOT MATCHED THEN
    INSERT (
      video_key, video_id, title, channel_title, category_id,
      publish_date, publish_year, publish_month, publish_day,
      tags, thumbnail_link, comments_disabled, ratings_disabled,
      video_error_or_removed, description, source_file, load_timestamp,
      version, is_current
    )
    VALUES (
      s.video_id, s.video_id, s.title, s.channel_title, s.category_id,
      s.publish_date, s.publish_year, s.publish_month, s.publish_day,
      s.tags, s.thumbnail_link, s.comments_disabled, s.ratings_disabled,
      s.video_error_or_removed, s.description, s.source_file, s.load_timestamp,
      1, TRUE
    );
    
  -- Insert new versions of updated records
  INSERT INTO DWH.DIM_VIDEO (
    video_key, video_id, title, channel_title, category_id,
    publish_date, publish_year, publish_month, publish_day,
    tags, thumbnail_link, comments_disabled, ratings_disabled,
    video_error_or_removed, description, source_file, load_timestamp,
    version, is_current
  )
  SELECT
    s.video_id, s.video_id, s.title, s.channel_title, s.category_id,
    s.publish_date, s.publish_year, s.publish_month, s.publish_day,
    s.tags, s.thumbnail_link, s.comments_disabled, s.ratings_disabled,
    s.video_error_or_removed, s.description, s.source_file, s.load_timestamp,
    t.version + 1, TRUE
  FROM RAW.STAGING_VIDEO_STREAM s
  JOIN DWH.DIM_VIDEO t
    ON t.video_id = s.video_id
    AND t.is_current = FALSE
    AND NOT EXISTS (
      SELECT 1 FROM DWH.DIM_VIDEO
      WHERE video_id = s.video_id AND is_current = TRUE
    );
    
  -- Update category dimension
  MERGE INTO DWH.DIM_CATEGORY t
  USING (
    SELECT
      category_id,
      category_name,
      assignable,
      --channel_id,
      etag,
      source_file,
      load_timestamp
    FROM RAW.STAGING_CATEGORY_STREAM
  ) s
  ON t.category_id = s.category_id AND t.is_current = TRUE
  WHEN MATCHED AND (
    t.category_name != s.category_name OR
    t.assignable != s.assignable OR
    --t.channel_id != s.channel_id
    t.etag != s.etag

  ) THEN
    UPDATE SET is_current = FALSE
  WHEN NOT MATCHED THEN
    INSERT (
      --category_id, category_name, assignable, channel_id,
      category_id, category_name, assignable, etag,
      source_file, load_timestamp, version, is_current
    )
    VALUES (
      --s.category_id, s.category_name, s.assignable, s.channel_id,
       s.category_id, s.category_name, s.assignable, s.etag,
      s.source_file, s.load_timestamp, 1, TRUE
    );
    
  -- Insert new versions of updated category records
  INSERT INTO DWH.DIM_CATEGORY (
    category_id, category_name, assignable, etag,
    source_file, load_timestamp, version, is_current
  )
  SELECT
    s.category_id, s.category_name, s.assignable, s.etag,
    s.source_file, s.load_timestamp, t.version + 1, TRUE
  FROM RAW.STAGING_CATEGORY_STREAM s
  JOIN DWH.DIM_CATEGORY t
    ON t.category_id = s.category_id
    AND t.is_current = FALSE
    AND NOT EXISTS (
      SELECT 1 FROM DWH.DIM_CATEGORY
      WHERE category_id = s.category_id AND is_current = TRUE
    );
END;

-- Create a task to update the fact table
CREATE OR REPLACE TASK RAW.UPDATE_FACT_TABLE
  WAREHOUSE = YOUTUBE_TRANSFORM_WH
  AFTER RAW.UPDATE_DIMENSIONS
AS
MERGE INTO DWH.FACT_VIDEO_STATS t
USING (
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
  FROM RAW.STAGING_VIDEO_STREAM v
  JOIN DWH.DIM_DATE d ON v.publish_date = d.full_date
) s
ON t.video_key = s.video_key AND t.trending_date = s.trending_date
WHEN MATCHED THEN
  UPDATE SET
    t.views = s.views,
    t.likes = s.likes,
    t.dislikes = s.dislikes,
    t.comment_count = s.comment_count,
    t.source_file = s.source_file,
    t.load_timestamp = s.load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    video_key, category_key, date_key, views, likes, dislikes,
    comment_count, trending_date, source_file, load_timestamp
  )
  VALUES (
    s.video_key, s.category_key, s.date_key, s.views, s.likes, s.dislikes,
    s.comment_count, s.trending_date, s.source_file, s.load_timestamp
  );

-- Create a task to run data quality checks
CREATE OR REPLACE TASK RAW.RUN_DATA_QUALITY
  WAREHOUSE = YOUTUBE_ANALYTICS_WH
  AFTER RAW.UPDATE_FACT_TABLE
AS
CALL RAW.RUN_DATA_QUALITY_CHECKS();

-- Enable the tasks
ALTER TASK RAW.RUN_DATA_QUALITY RESUME;
ALTER TASK RAW.UPDATE_FACT_TABLE RESUME;
ALTER TASK RAW.UPDATE_DIMENSIONS RESUME;
ALTER TASK RAW.PROCESS_NEW_CATEGORIES RESUME;
ALTER TASK RAW.PROCESS_NEW_VIDEOS RESUME;
ALTER TASK RAW.MONITOR_NEW_FILES RESUME;


-- Create a task to refresh analytics views
CREATE OR REPLACE TASK RAW.REFRESH_ANALYTICS_VIEWS
  WAREHOUSE = YOUTUBE_ANALYTICS_WH
  AFTER RAW.RUN_DATA_QUALITY
AS
BEGIN
  -- Refresh materialized views
  ALTER MATERIALIZED VIEW ANALYTICS.DAILY_VIDEO_METRICS REFRESH;
  
  -- Log completion
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('ANALYTICS_REFRESH', 'REFRESH_VIEWS', 'SUCCESS', 0, NULL);
END;

-- Enable the new task
ALTER TASK RAW.REFRESH_ANALYTICS_VIEWS RESUME;
