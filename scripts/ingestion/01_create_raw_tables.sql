-- YouTube Analytics Project - Create Raw Tables

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;

-- Create raw table for videos data
CREATE OR REPLACE TABLE RAW.VIDEOS (
  video_id VARCHAR(255),
  trending_date VARCHAR(255),
  title VARCHAR(1000),
  channel_title VARCHAR(255),
  category_id NUMBER,
  publish_time TIMESTAMP_NTZ,
  tags VARCHAR(5000),
  views NUMBER,
  likes NUMBER,
  dislikes NUMBER,
  comment_count NUMBER,
  thumbnail_link VARCHAR(255),
  comments_disabled BOOLEAN,
  ratings_disabled BOOLEAN,
  video_error_or_removed BOOLEAN,
  description VARCHAR(5000),
  source_file VARCHAR(255),
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create raw table for category data (from JSON)
CREATE OR REPLACE TABLE RAW.CATEGORIES (
  raw_json VARIANT,
  source_file VARCHAR(255),
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create metadata table to track loaded files
CREATE OR REPLACE TABLE RAW.LOADED_FILES (
  file_name VARCHAR(255),
  file_size NUMBER,
  row_count NUMBER,
  load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  status VARCHAR(50),
  CONSTRAINT PK_LOADED_FILES PRIMARY KEY (file_name)
);

-- Create procedure to log pipeline execution
-- CREATE OR REPLACE PROCEDURE RAW.LOG_PIPELINE_STEP(
--   PIPELINE_NAME VARCHAR,
--   STEP_NAME VARCHAR,
--   STATUS VARCHAR,
--   ROW_COUNT NUMBER,
--   ERROR_MESSAGE VARCHAR
-- )
-- RETURNS VARCHAR
-- LANGUAGE JAVASCRIPT
-- AS
-- $$
--   var start_time_sql = "INSERT INTO YOUTUBE_ANALYTICS.RAW.PIPELINE_LOG(PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE) VALUES (?, ?, ?, ?, ?)";
--   var start_stmt = snowflake.createStatement({
--     sqlText: start_time_sql,
--     binds: [PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE]
--   });
--   start_stmt.execute();
--   return "Logged pipeline step: " + PIPELINE_NAME + " - " + STEP_NAME;
-- $$;



CREATE OR REPLACE PROCEDURE RAW.LOG_PIPELINE_STEP(
  PIPELINE_NAME VARCHAR,
  STEP_NAME VARCHAR,
  STATUS VARCHAR,
  ROW_COUNT NUMBER,
  ERROR_MESSAGE VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  // Use string concatenation instead of binds to avoid type issues
  var sql = "INSERT INTO YOUTUBE_ANALYTICS.RAW.PIPELINE_LOG(PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE) VALUES ('" + 
    PIPELINE_NAME + "', '" + 
    STEP_NAME + "', '" + 
    STATUS + "', " + 
    (ROW_COUNT === null ? "NULL" : ROW_COUNT) + ", " + 
    (ERROR_MESSAGE === null ? "NULL" : "'" + ERROR_MESSAGE.replace(/'/g, "''") + "'") + 
    ")";
  
  try {
    var stmt = snowflake.createStatement({sqlText: sql});
    stmt.execute();
    return "Logged pipeline step: " + PIPELINE_NAME + " - " + STEP_NAME;
  } catch (err) {
    return "Error logging pipeline step: " + err.message + "\nSQL: " + sql;
  }
$$;

CREATE TABLE IF NOT EXISTS YOUTUBE_ANALYTICS.RAW.PIPELINE_LOG (
  PIPELINE_RUN_ID NUMBER IDENTITY(1,1),
  PIPELINE_NAME VARCHAR(100),
  STEP_NAME VARCHAR(100),
  STATUS VARCHAR(20),
  RECORDS_PROCESSED NUMBER,
  ERROR_MESSAGE VARCHAR(1000),
  START_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  END_TIME TIMESTAMP_LTZ,
  DURATION_SECONDS NUMBER
);





