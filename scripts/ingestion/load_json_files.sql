-- YouTube Analytics Project - Load JSON Files
-- Run this script directly in Snowflake

-- Set context
USE DATABASE YOUTUBE_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE YOUTUBE_LOAD_WH;

-- Create a log entry for start
INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
VALUES ('DATA_INGESTION', 'LOAD_CATEGORIES_START', 'RUNNING', 0, NULL);



-- Set file format to JSON
ALTER STAGE YOUTUBE_EXTERNAL_STAGE SET FILE_FORMAT = RAW.JSON_FORMAT;

-- Load all JSON files
COPY INTO RAW.CATEGORIES (raw_json, source_file)
FROM (
  SELECT 
    $1,
    METADATA$FILENAME
  FROM @YOUTUBE_EXTERNAL_STAGE/
)
FILE_FORMAT = RAW.JSON_FORMAT
PATTERN = '.*_category_id\.json\.gz'
ON_ERROR = 'CONTINUE';

-- Reset file format to CSV for future operations
ALTER STAGE YOUTUBE_EXTERNAL_STAGE SET FILE_FORMAT = RAW.CSV_FORMAT;

-- Check the count of loaded records
SELECT COUNT(*) FROM RAW.CATEGORIES;

-- -- Record loaded files (simplified approach)
-- INSERT INTO RAW.LOADED_FILES (file_name, row_count, status)
-- SELECT 
--   REGEXP_SUBSTR(metadata$filename, '[^/]+$') as file_name,
--   COUNT(*) as row_count,
--   'LOADED' as status
-- FROM @YOUTUBE_EXTERNAL_STAGE/*.json
-- GROUP BY 1;



-- Create a log entry for completion
INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
VALUES ('DATA_INGESTION', 'LOAD_CATEGORIES_COMPLETE', 'SUCCESS', $rows_loaded, NULL);

-- Show loaded data count
SELECT COUNT(*) AS ROWS_LOADED FROM RAW.CATEGORIES;

=============================================

