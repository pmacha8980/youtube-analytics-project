CREATE OR REPLACE PROCEDURE YOUTUBE_ANALYTICS.RAW.RUN_DATA_QUALITY_CHECKS_SQL()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  fail_count INTEGER;
BEGIN
  -- Log start
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('DATA_QUALITY', 'START_CHECKS', 'RUNNING', 0, NULL);
  
  -- Check 1: Null key values in fact table
  SELECT COUNT(*) INTO :fail_count
  FROM DWH.FACT_VIDEO_STATS
  WHERE video_key IS NULL OR category_key IS NULL;
  
  -- Log result
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('DATA_QUALITY', 'NULL_KEY_CHECK', 
          IFF(:fail_count > 0, 'FAILED', 'PASSED'),
          :fail_count,
          IFF(:fail_count > 0, 'Found records with null keys', NULL));
  
  -- Check 2: Videos without categories
  SELECT COUNT(*) INTO :fail_count
  FROM DWH.FACT_VIDEO_STATS f
  LEFT JOIN DWH.DIM_CATEGORY c ON f.category_key = c.category_id
  WHERE c.category_id IS NULL;
  
  -- Log result
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('DATA_QUALITY', 'MISSING_CATEGORY_CHECK', 
          IFF(:fail_count > 0, 'FAILED', 'PASSED'),
          :fail_count,
          IFF(:fail_count > 0, 'Found videos without valid categories', NULL));
  
  -- Log completion
  INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
  VALUES ('DATA_QUALITY', 'COMPLETE', 'SUCCESS', 0, NULL);
  
  RETURN 'Data quality checks completed successfully';
EXCEPTION
  WHEN OTHER THEN
    INSERT INTO RAW.PIPELINE_LOG (PIPELINE_NAME, STEP_NAME, STATUS, ROW_COUNT, ERROR_MESSAGE)
    VALUES ('DATA_QUALITY', 'ERROR', 'FAILED', 0, 'SQL Error: ' || SQLSTATE || ' - ' || SQLERRM);
    RETURN 'Error running data quality checks: ' || SQLSTATE || ' - ' || SQLERRM;
END;
$$;


CALL YOUTUBE_ANALYTICS.RAW.RUN_DATA_QUALITY_CHECKS_SQL();

