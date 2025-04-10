-- File: /Users/prags/Documents/GitHub/youtube-analytics-project/scripts/setup/03_setup_snowpipe.sql

-- YouTube Analytics Project - Setup Snowpipe for Automated Ingestion

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;


-- CREATE OR REPLACE STORAGE INTEGRATION s3_youtube_integration
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-access-role'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://youtube-analytics-data/');

-- Note: After creating the integration, you need to retrieve the IAM user and
-- external ID to configure your cloud storage notifications:
-- DESC INTEGRATION s3_youtube_integration;

-- Create or replace the external stage
CREATE OR REPLACE STAGE YOUTUBE_EXTERNAL_STAGE
  -- URL = 's3://youtube-analytics-data/'
  -- STORAGE_INTEGRATION = s3_youtube_integration
  FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create Snowpipe for video data
CREATE OR REPLACE PIPE RAW.VIDEO_SNOWPIPE
  AUTO_INGEST = TRUE
  AS
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
    FROM @YOUTUBE_EXTERNAL_STAGE/videos/
  )
  FILE_FORMAT = RAW.CSV_FORMAT
  PATTERN = '.*videos\.csv\.gz'
  ON_ERROR = 'CONTINUE';

-- Create Snowpipe for category data
CREATE OR REPLACE PIPE RAW.CATEGORY_SNOWPIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW.CATEGORIES (raw_json, source_file)
  FROM (
    SELECT 
      $1,
      METADATA$FILENAME
    FROM @YOUTUBE_EXTERNAL_STAGE/categories/
  )
  FILE_FORMAT = RAW.JSON_FORMAT
  PATTERN = '.*_category_id\.json\.gz'
  ON_ERROR = 'CONTINUE';

-- Show pipe status
SHOW PIPES;

-- Log setup completion
INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, RECORDS_PROCESSED, ERROR_MESSAGE)
VALUES ('SETUP', 'SNOWPIPE_SETUP', 'SUCCESS', 0, NULL);