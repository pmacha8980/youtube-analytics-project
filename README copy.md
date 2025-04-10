# YouTube Analytics Project

This project implements an end-to-end data solution for YouTube video statistics using Snowflake.

## Architecture Overview

The project follows an ELT (Extract, Load, Transform) approach with automated data ingestion using Snowpipe.

## Project Structure


youtube_analytics_project/
├── scripts/                 # Snowflake SQL scripts
│   ├── setup/               # Initial setup scripts
│   │   ├── 01_create_environment.sql
│   │   ├── 02_create_environments.sql
│   │   └── 03_setup_snowpipe.sql
│   ├── ingestion/           # Data loading scripts
│   ├── transformation/      # Data modeling scripts
│   │   ├── 01_create_staging_tables.sql
│   │   └── 02_create_dimension_tables.sql
│   ├── analytics/           # Analytics views
│   │   ├── 01_create_analytics_views.sql
│   │   ├── 02_create_top_videos_view.sql
│   │   └── 03_create_materialized_views.sql
│   ├── orchestration/       # Pipeline orchestration scripts
│   │   └── 01_setup_orchestration.sql
│   └── data_quality/        # Data quality checks
├── docs/                    # Documentation and diagrams
│   ├── architecture.md      # Architecture overview
│   └── orchestration_diagram.txt  # Orchestration diagram
├── deployment/              # Deployment scripts
└── data/                    # Sample data and utilities

## Getting Started

### Prerequisites

1. Snowflake account with ACCOUNTADMIN privileges
2. SnowSQL CLI installed
3. Cloud storage account (AWS S3, Azure Blob Storage, or GCP Cloud Storage)

### Setup Instructions

#### 1. Initial Setup

bash
# Create the database, schemas, and roles
snowsql -f scripts/setup/01_create_environment.sql

# Create multi-environment support
snowsql -f scripts/setup/02_create_environments.sql

#### 2. Data Ingestion Setup

For the lab demonstration, manually uploaded files to the Snowflake stage:

bash
# Upload sample data files to Snowflake stage
snowsql -c my_connection -q "PUT file:///path/to/videos.csv @YOUTUBE_EXTERNAL_STAGE/videos/ AUTO_COMPRESS=TRUE"
snowsql -c my_connection -q "PUT file:///path/to/category_id.json @YOUTUBE_EXTERNAL_STAGE/categories/ AUTO_COMPRESS=
TRUE"

For production use with daily data feeds, set up Snowpipe:

bash
# Set up Snowpipe for automated ingestion
snowsql -f scripts/setup/03_setup_snowpipe.sql

After running this script, you'll need to configure your cloud storage:

1. Get the Snowpipe notification channel:
   bash
  snowsql -c my_connection -q "SHOW PIPES IN DATABASE YOUTUBE_ANALYTICS"
  


2. Configure cloud storage event notifications:
   - For AWS S3: Set up S3 event notifications to the Snowpipe SQS queue
   - For Azure: Configure Event Grid notifications
   - For GCP: Set up Cloud Storage notifications

#### 3. Create Transformation Layer


bash
# Create staging tables
snowsql -f scripts/transformation/01_create_staging_tables.sql

# Create dimension tables
snowsql -f scripts/transformation/02_create_dimension_tables.sql

#### 4. Create Analytics Views

bash
# Create analytics views
snowsql -f scripts/analytics/01_create_analytics_views.sql
snowsql -f scripts/analytics/02_create_top_videos_view.sql
snowsql -f scripts/analytics/03_create_materialized_views.sql

#### 5. Set Up Orchestration

bash
# Set up orchestration tasks
snowsql -f scripts/orchestration/01_setup_orchestration.sql

## Data Model

The project uses a star schema design:

- **Fact Table**: FACT_VIDEO_STATS
- **Dimension Tables**: DIM_VIDEO, DIM_CATEGORY, DIM_DATE

## Automated Data Pipeline

The data pipeline is fully automated using Snowflake tasks and Snowpipe:

1. **Data Ingestion**: Files uploaded to cloud storage are automatically loaded via Snowpipe
2. **Transformation**: Snowflake tasks process the data through staging to the dimensional model
3. **Analytics**: Materialized views are automatically refreshed for reporting

## Monitoring and Maintenance

Monitor the pipeline using:

sql
-- Check Snowpipe status
SELECT  FROM TABLE(INFORMATIONSCHEMA.PIPE_USAGE_HISTORY());

-- Check task execution history
SELECT  FROM TABLE(INFORMATIONSCHEMA.TASK_HISTORY());

-- Check pipeline logs
SELECT  FROM RAW.PIPELINELOG ORDER BY TIMESTAMP DESC LIMIT 100;

## Scaling for Production

This architecture is designed to scale for production use:

- **Snowpipe** handles large volumes of incoming data
- **Streams** enable incremental processing
- **Tasks** automate the entire pipeline
- **Materialized Views** provide efficient analytics
