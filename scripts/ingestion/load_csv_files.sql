-- YouTube Analytics Project - Load CSV Files
-- Run this script directly in Snowflake

-- Set context
USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;

-- Create a log entry for start
INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
VALUES ('DATA_INGESTION', 'LOAD_VIDEOS_START', 'RUNNING', 0, NULL);

-- List files in stage
LIST @YOUTUBE_EXTERNAL_STAGE/*.csv;

-- Create a new file format with pipe delimiter
CREATE OR REPLACE FILE FORMAT RAW.PIPE_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'  -- Changed from comma to pipe
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;



-- Load the CSV files with the new file format
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
FILE_FORMAT = RAW.PIPE_CSV_FORMAT  -- Using the new file format
PATTERN = '.*videos\.csv\.gz'
ON_ERROR = 'CONTINUE';

-- Check the count of loaded records
SELECT COUNT(*) FROM RAW.VIDEOS;

-- -- Record loaded files (simplified approach)
-- INSERT INTO RAW.LOADED_FILES (file_name, row_count, status)
-- SELECT 
--   REGEXP_SUBSTR(metadata$filename, '[^/]+$') as file_name,
--   COUNT(*) as row_count,
--   'LOADED' as status
-- FROM @YOUTUBE_EXTERNAL_STAGE/*.csv
-- GROUP BY 1;

-- Create a log entry for completion
INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
VALUES ('DATA_INGESTION', 'LOAD_VIDEOS_COMPLETE', 'SUCCESS', $rows_loaded, NULL);

-- Show loaded data count
SELECT COUNT(*) AS ROWS_LOADED FROM RAW.VIDEOS;

