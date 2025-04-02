-- YouTube Analytics Project - Data Quality Checks

USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_ANALYTICS_WH;

-- Create a table to store data quality check results
CREATE OR REPLACE TABLE RAW.DATA_QUALITY_RESULTS (
  check_id NUMBER IDENTITY(1,1),
  check_name VARCHAR(100),
  check_description VARCHAR(500),
  table_name VARCHAR(100),
  column_name VARCHAR(100),
  check_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  check_status VARCHAR(20),
  failed_records NUMBER,
  total_records NUMBER,
  error_message VARCHAR(1000)
);

-- Create a procedure to run data quality checks
CREATE OR REPLACE PROCEDURE RAW.RUN_DATA_QUALITY_CHECKS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  // Array of checks to run
  var checks = [
    {
      name: "CATEGORY_ID_VALIDATION",
      description: "Validate that all video category_ids match the list of categories in JSON files",
      query: `
        INSERT INTO RAW.DATA_QUALITY_RESULTS (
          check_name, check_description, table_name, column_name, 
          check_status, failed_records, total_records, error_message
        )
        SELECT
          'CATEGORY_ID_VALIDATION',
          'Validate that all video category_ids match the list of categories in JSON files',
          'STAGING.VIDEOS',
          'category_id',
          CASE WHEN COUNT_IF(validation_status = 'INVALID') > 0 THEN 'FAILED' ELSE 'PASSED' END,
          COUNT_IF(validation_status = 'INVALID'),
          COUNT(*),
          CASE WHEN COUNT_IF(validation_status = 'INVALID') > 0 
               THEN 'Found ' || COUNT_IF(validation_status = 'INVALID') || ' videos with invalid category IDs'
               ELSE NULL
          END
        FROM STAGING.CATEGORY_VALIDATION;
      `
    },
    {
      name: "NULL_VIDEO_ID_CHECK",
      description: "Check for NULL video_ids in the videos table",
      query: `
        INSERT INTO RAW.DATA_QUALITY_RESULTS (
          check_name, check_description, table_name, column_name, 
          check_status, failed_records, total_records, error_message
        )
        SELECT
          'NULL_VIDEO_ID_CHECK',
          'Check for NULL video_ids in the videos table',
          'STAGING.VIDEOS',
          'video_id',
          CASE WHEN COUNT_IF(video_id IS NULL) > 0 THEN 'FAILED' ELSE 'PASSED' END,
          COUNT_IF(video_id IS NULL),
          COUNT(*),
          CASE WHEN COUNT_IF(video_id IS NULL) > 0 
               THEN 'Found ' || COUNT_IF(video_id IS NULL) || ' records with NULL video_id'
               ELSE NULL
          END
        FROM STAGING.VIDEOS;
      `
    },
    {
      name: "NEGATIVE_VIEWS_CHECK",
      description: "Check for negative view counts",
      query: `
        INSERT INTO RAW.DATA_QUALITY_RESULTS (
          check_name, check_description, table_name, column_name, 
          check_status, failed_records, total_records, error_message
        )
        SELECT
          'NEGATIVE_VIEWS_CHECK',
          'Check for negative view counts',
          'STAGING.VIDEOS',
          'views',
          CASE WHEN COUNT_IF(views < 0) > 0 THEN 'FAILED' ELSE 'PASSED' END,
          COUNT_IF(views < 0),
          COUNT(*),
          CASE WHEN COUNT_IF(views < 0) > 0 
               THEN 'Found ' || COUNT_IF(views < 0) || ' records with negative view counts'
               ELSE NULL
          END
        FROM STAGING.VIDEOS;
      `
    },
    {
      name: "FUTURE_PUBLISH_DATE_CHECK",
      description: "Check for publish dates in the future",
      query: `
        INSERT INTO RAW.DATA_QUALITY_RESULTS (
          check_name, check_description, table_name, column_name, 
          check_status, failed_records, total_records, error_message
        )
        SELECT
          'FUTURE_PUBLISH_DATE_CHECK',
          'Check for publish dates in the future',
          'STAGING.VIDEOS',
          'publish_time',
          CASE WHEN COUNT_IF(publish_time > CURRENT_TIMESTAMP()) > 0 THEN 'FAILED' ELSE 'PASSED' END,
          COUNT_IF(publish_time > CURRENT_TIMESTAMP()),
          COUNT(*),
          CASE WHEN COUNT_IF(publish_time > CURRENT_TIMESTAMP()) > 0 
               THEN 'Found ' || COUNT_IF(publish_time > CURRENT_TIMESTAMP()) || ' records with future publish dates'
               ELSE NULL
          END
        FROM STAGING.VIDEOS;
      `
    },
    {
      name: "DUPLICATE_VIDEO_TRENDING_CHECK",
      description: "Check for duplicate video_id and trending_date combinations",
      query: `
        INSERT INTO RAW.DATA_QUALITY_RESULTS (
          check_name, check_description, table_name, column_name, 
          check_status, failed_records, total_records, error_message
        )
        WITH duplicates AS (
          SELECT 
            video_id, 
            trending_date, 
            COUNT(*) as cnt
          FROM STAGING.VIDEOS
          GROUP BY video_id, trending_date
          HAVING COUNT(*) > 1
        )
        SELECT
          'DUPLICATE_VIDEO_TRENDING_CHECK',
          'Check for duplicate video_id and trending_date combinations',
          'STAGING.VIDEOS',
          'video_id,trending_date',
          CASE WHEN COUNT(*) > 0 THEN 'FAILED' ELSE 'PASSED' END,
          SUM(cnt) - COUNT(*),
          (SELECT COUNT(*) FROM STAGING.VIDEOS),
          CASE WHEN COUNT(*) > 0 
               THEN 'Found ' || COUNT(*) || ' duplicate video_id and trending_date combinations'
               ELSE NULL
          END
        FROM duplicates;
      `
    }
  ];
  
  // Run each check
  var results = [];
  for (var i = 0; i < checks.length; i++) {
    try {
      var stmt = snowflake.createStatement({sqlText: checks[i].query});
      var result = stmt.execute();
      results.push(checks[i].name + ": Success");
    }
    catch (err) {
      results.push(checks[i].name + ": Error - " + err.message);
      
      // Log the error
      var log_error = snowflake.createStatement({
        sqlText: "CALL RAW.LOG_PIPELINE_STEP('DATA_QUALITY', ?, 'ERROR', 0, ?)",
        binds: [checks[i].name, err.message]
      });
      log_error.execute();
    }
  }
  
  // Log completion
  var log_complete = snowflake.createStatement({
    sqlText: "CALL RAW.LOG_PIPELINE_STEP('DATA_QUALITY', 'ALL_CHECKS', 'COMPLETE', ?, NULL)",
    binds: [checks.length]
  });
  log_complete.execute();
  
  return "Completed " + checks.length + " data quality checks. Results: " + results.join("; ");
$$;

-- Create a task to run data quality checks regularly
CREATE OR REPLACE TASK RAW.RUN_DATA_QUALITY_TASK
  WAREHOUSE = YOUTUBE_ANALYTICS_WH
  SCHEDULE = 'USING CRON 0 0 * * * America/Los_Angeles' -- Run daily at midnight
AS
CALL RAW.RUN_DATA_QUALITY_CHECKS();

-- Enable the task
ALTER TASK RAW.RUN_DATA_QUALITY_TASK RESUME;
