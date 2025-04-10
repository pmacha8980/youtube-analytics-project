-- YouTube Analytics Project - Setup Data Simulation for Daily Feeds

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;

-- Create a stored procedure to simulate daily data feeds
CREATE OR REPLACE PROCEDURE RAW.SIMULATE_DAILY_FEED()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result STRING;
BEGIN
  -- Log the start of the simulation
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, RECORDS_PROCESSED, ERROR_MESSAGE)
  VALUES ('SIMULATION', 'DAILY_FEED_START', 'RUNNING', 0, NULL);
  
  -- Create a temporary table with "new" data
  -- This simulates new data coming in daily
  CREATE OR REPLACE TEMPORARY TABLE RAW.TEMP_NEW_VIDEOS AS
  SELECT
    video_id || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD') AS video_id,
    TO_VARCHAR(CURRENT_DATE(), 'YY.DD.MM') AS trending_date,
    title || ' [Updated]' AS title,
    channel_title,
    category_id,
    CURRENT_TIMESTAMP() AS publish_time,
    tags,
    views * (0.9 + RANDOM() * 0.2) AS views,  -- Randomize views
    likes * (0.9 + RANDOM() * 0.2) AS likes,  -- Randomize likes
    dislikes * (0.9 + RANDOM() * 0.2) AS dislikes,  -- Randomize dislikes
    comment_count * (0.9 + RANDOM() * 0.2) AS comment_count,  -- Randomize comments
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    'SIMULATED_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD') AS source_file
  FROM RAW.VIDEOS
  WHERE ROWNUM() <= 50  -- Only copy a few rows for demonstration
  AND source_file NOT LIKE 'SIMULATED_%';  -- Don't copy already simulated data
  
  -- Insert the "new" data into the videos table
  INSERT INTO RAW.VIDEOS
  SELECT * FROM RAW.TEMP_NEW_VIDEOS;
  
  -- Get the number of records inserted
  LET records_inserted := SQLROWCOUNT;
  
  -- Log the completion of the simulation
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, RECORDS_PROCESSED, ERROR_MESSAGE)
  VALUES ('SIMULATION', 'DAILY_FEED_COMPLETE', 'SUCCESS', :records_inserted, NULL);
  
  RETURN 'Simulated daily feed with ' || :records_inserted || ' new records';
END;
$$;

-- Create a task to simulate daily data feeds
CREATE OR REPLACE TASK RAW.SIMULATE_DAILY_FEED_TASK
  WAREHOUSE = YOUTUBE_LOAD_WH
  SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles'  -- Run daily at midnight
AS
CALL RAW.SIMULATE_DAILY_FEED();

-- Enable the task
ALTER TASK RAW.SIMULATE_DAILY_FEED_TASK RESUME;
