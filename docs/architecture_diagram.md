# YouTube Analytics Project Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     YOUTUBE ANALYTICS ARCHITECTURE                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                        
┌─────────────────────────────────────────────────────┐                  ┌─────────────────────────────────────────────────┐
│              DATA SOURCES                           │                  │              DATA INGESTION                      │
│                                                     │                  │                                                 │
│  ┌───────────────────┐      ┌───────────────────┐   │                  │  ┌───────────────────┐      ┌───────────────────┐│
│  │   YouTube CSV     │      │  YouTube JSON     │   │                  │  │  Snowflake        │      │    Snowflake      ││
│  │   Files           │      │  Category Files   │   │                  │  │  External Stage   │      │    Snowpipe       ││
│  │   (Video Stats)   │      │   (Metadata)      │   │                  │  │                   │      │    (Optional)     ││
│  └───────┬───────────┘      └────────┬──────────┘   │                  │  └────────┬──────────┘      └─────────┬─────────┘│
│          │                           │              │                  │           │                           │          │
└──────────┼───────────────────────────┼──────────────┘                  └───────────┼───────────────────────────┼──────────┘
           │                           │                                             │                           │           
           ▼                           ▼                                             ▼                           ▼           
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                 RAW LAYER                                                                 │
│                                                                                                                          │
│  ┌───────────────────────────┐                                          ┌───────────────────────────┐                     │
│  │                           │                                          │                           │                     │
│  │     RAW.VIDEOS            │                                          │     RAW.CATEGORIES        │                     │
│  │     (Original CSV Data)   │                                          │     (Original JSON Data)  │                     │
│  │                           │                                          │                           │                     │
│  └─────────────┬─────────────┘                                          └─────────────┬─────────────┘                     │
│                │                                                                      │                                   │
│                │                        ┌───────────────────────────┐                 │                                   │
│                │                        │                           │                 │                                   │
│                └───────────────────────▶│     STREAMS               │◀────────────────┘                                   │
│                                         │     (Change Tracking)     │                                                     │
│                                         │                           │                                                     │
│                                         └────────────┬──────────────┘                                                     │
└─────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┘
                                                      │                                                                      
                                                      ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                STAGING LAYER                                                              │
│                                                                                                                          │
│  ┌───────────────────────────┐                                          ┌───────────────────────────┐                     │
│  │                           │                                          │                           │                     │
│  │     STAGING.VIDEOS        │                                          │     STAGING.CATEGORIES    │                     │
│  │     (Cleansed Data)       │                                          │     (Parsed JSON)         │                     │
│  │                           │                                          │                           │                     │
│  └─────────────┬─────────────┘                                          └─────────────┬─────────────┘                     │
│                │                                                                      │                                   │
│                │                        ┌───────────────────────────┐                 │                                   │
│                │                        │                           │                 │                                   │
│                └───────────────────────▶│     STREAMS               │◀────────────────┘                                   │
│                                         │     (Change Tracking)     │                                                     │
│                                         │                           │                                                     │
│                                         └────────────┬──────────────┘                                                     │
└─────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┘
                                                      │                                                                      
                                                      ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           WAREHOUSE LAYER (DWH)                                                           │
│                                                                                                                          │
│  ┌───────────────────┐      ┌───────────────────┐      ┌───────────────────┐      ┌───────────────────────────┐          │
│  │                   │      │                   │      │                   │      │                           │          │
│  │   DIM_VIDEO       │      │   DIM_CATEGORY    │      │   DIM_DATE        │      │   FACT_VIDEO_STATS        │          │
│  │   (SCD Type 2)    │      │   (SCD Type 2)    │      │   (Date Dimension)│      │   (Metrics & Measures)    │          │
│  │                   │      │                   │      │                   │      │                           │          │
│  └───────────────────┘      └───────────────────┘      └───────────────────┘      └───────────────────────────┘          │
│                                                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                      │                                                                      
                                                      ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              ANALYTICS LAYER                                                              │
│                                                                                                                          │
│  ┌───────────────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐                     │
│  │                           │      │                           │      │                           │                     │
│  │   DAILY_VIDEO_METRICS     │      │   CATEGORY_PERFORMANCE    │      │   CHANNEL_PERFORMANCE     │                     │
│  │   (Materialized View)     │      │   (Materialized View)     │      │   (Materialized View)     │                     │
│  │                           │      │                           │      │                           │                     │
│  └───────────────────────────┘      └───────────────────────────┘      └───────────────────────────┘                     │
│                                                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                      │                                                                      
                                                      ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                           ORCHESTRATION & MONITORING                                                      │
│                                                                                                                          │
│  ┌───────────────────────────┐      ┌───────────────────────────┐      ┌───────────────────────────┐                     │
│  │                           │      │                           │      │                           │                     │
│  │   SNOWFLAKE TASKS         │      │   DATA QUALITY CHECKS     │      │   PIPELINE_LOG            │                     │
│  │   (Workflow Automation)   │      │   (Validation)            │      │   (Monitoring)            │                     │
│  │                           │      │                           │      │                           │                     │
│  └───────────────────────────┘      └───────────────────────────┘      └───────────────────────────┘                     │
│                                                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                      │                                                                      
                                                      ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                VISUALIZATION                                                              │
│                                                                                                                          │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                                                                   │   │
│  │                                         SNOWSIGHT DASHBOARDS                                                      │   │
│  │                                                                                                                   │   │
│  │   ┌───────────────────┐      ┌───────────────────┐      ┌───────────────────┐      ┌───────────────────┐         │   │
│  │   │                   │      │                   │      │                   │      │                   │         │   │
│  │   │  Top Videos by    │      │  Most Disliked    │      │  Category Likes   │      │  Videos by       │         │   │
│  │   │  Category         │      │  Videos           │      │  Analysis         │      │  Source File     │         │   │
│  │   │                   │      │                   │      │                   │      │                   │         │   │
│  │   └───────────────────┘      └───────────────────┘      └───────────────────┘      └───────────────────┘         │   │
│  │                                                                                                                   │   │
│  └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Architecture Components Explanation

### 1. Data Sources
- **YouTube CSV Files**: Contains video statistics data (views, likes, dislikes, etc.)
- **YouTube JSON Files**: Contains category metadata information

### 2. Data Ingestion
- **Snowflake External Stage**: Storage location for source files
- **Snowpipe** (Optional): For automated continuous loading of new data

### 3. Raw Layer
- **RAW.VIDEOS**: Stores raw CSV data as loaded from source
- **RAW.CATEGORIES**: Stores raw JSON data as loaded from source
- **Streams**: Track changes to raw tables for incremental processing

### 4. Staging Layer
- **STAGING.VIDEOS**: Cleansed and validated video data
- **STAGING.CATEGORIES**: Parsed JSON data with extracted category information
- **Streams**: Track changes to staging tables for incremental processing

### 5. Warehouse Layer (DWH)
- **DIM_VIDEO**: Video dimension with SCD Type 2 versioning
- **DIM_CATEGORY**: Category dimension with SCD Type 2 versioning
- **DIM_DATE**: Date dimension with calendar attributes
- **FACT_VIDEO_STATS**: Fact table with metrics (views, likes, dislikes)

### 6. Analytics Layer
- **DAILY_VIDEO_METRICS**: Aggregated daily metrics
- **CATEGORY_PERFORMANCE**: Category-level performance metrics
- **CHANNEL_PERFORMANCE**: Channel-level performance metrics

### 7. Orchestration & Monitoring
- **Snowflake Tasks**: Automated workflow for data processing
- **Data Quality Checks**: Validation of data integrity
- **PIPELINE_LOG**: Logging and monitoring of pipeline execution

### 8. Visualization
- **Snowsight Dashboards**: Interactive visualizations showing insights from the data
  - Top Videos by Category
  - Most Disliked Videos
  - Category Likes Analysis
  - Videos by Source File

## Data Flow

1. Source files are loaded into the Snowflake External Stage
2. Raw data is copied into RAW schema tables
3. Streams track changes in raw tables
4. Tasks process data from raw to staging, applying cleansing and validation
5. Streams track changes in staging tables
6. Tasks process data from staging to dimensional model (DWH schema)
7. Analytics views are refreshed based on the dimensional model
8. Dashboards visualize the analytics data

This architecture follows ELT (Extract, Load, Transform) principles, with all processing happening within Snowflake. It provides data lineage, auditability, and a clear separation of concerns between the different data layers.

## Key Features

- **Scalability**: Designed to handle growing data volumes
- **Incremental Processing**: Uses streams for efficient change data capture
- **Automation**: Tasks orchestrate the entire pipeline
- **Data Quality**: Built-in validation checks
- **Monitoring**: Comprehensive logging for troubleshooting
- **Flexibility**: Clear separation of layers allows for independent evolution

## Production Considerations

For a production implementation, consider:

1. **Adding Primary and Foreign Key Constraints**: Enhances data integrity
2. **Implementing Multi-Environment Support**: Dev, Test, Prod environments
3. **Setting Up CI/CD Pipeline**: For automated testing and deployment
4. **Enhancing Error Handling**: More sophisticated error recovery
5. **Implementing Data Retention Policies**: For managing historical data
6. **Adding Row-Level Security**: For more granular access control
