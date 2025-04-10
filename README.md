# YouTube Analytics Project

This project implements an end-to-end data solution for YouTube video statistics using Snowflake.

## Architecture Overview

The project follows an ELT (Extract, Load, Transform) approach with all processing happening within Snowflake:

- **Data Ingestion**: Initial data load into Snowflake with simulation of daily feeds
- **Transformation**: Multi-layer data model (Raw → Staging → Dimensional)
- **Analytics**: Materialized views for business insights
- **Orchestration**: Automated pipeline using Snowflake tasks

## Project Structure


youtube_analytics_project/
├── scripts/                 # Snowflake SQL scripts
│   ├── setup/               # Initial setup scripts
│   │   ├── 01_create_environment.sql
│   │   ├── 02_create_environments.sql
│   │   └── 03_setup_data_simulation.sql
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
└── data/                    # Sample data files

## Getting Started

### Prerequisites

1. Snowflake account with appropriate privileges
2. SnowSQL CLI installed
3. Sample YouTube data files

### Setup Instructions

#### 1. Initial Setup

bash
# Create the database, schemas, and roles
snowsql -f scripts/setup/01_create_environment.sql

# Create multi-environment support
snowsql -f scripts/setup/02_create_environments.sql

#### 2. Data Ingestion Setup

For the lab demonstration, you can manually upload files to Snowflake:

bash
# Upload sample data files to Snowflake
snowsql -c my_connection -q "PUT file:///path/to/videos.csv @YOUTUBE_EXTERNAL_STAGE AUTO_COMPRESS=TRUE"
snowsql -c my_connection -q "PUT file:///path/to/category_id.json @YOUTUBE_EXTERNAL_STAGE AUTO_COMPRESS=TRUE"

Set up data simulation for daily feeds:

bash
# Set up data simulation
snowsql -f scripts/setup/03_setup_data_simulation.sql

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

The data pipeline is fully automated using Snowflake tasks:

1. **Data Ingestion**: Simulated daily data feeds
2. **Transformation**: Data flows through staging to dimensional model
3. **Analytics**: Materialized views are automatically refreshed

## Monitoring and Maintenance

Monitor the pipeline using:

sql
-- Check task execution history
SELECT  FROM TABLE(INFORMATIONSCHEMA.TASK_HISTORY());

-- Check pipeline logs
SELECT  FROM RAW.PIPELINELOG ORDER BY TIMESTAMP DESC LIMIT 100;

## Scaling for Production

This architecture is designed to scale for production use:

- **Cloud Storage Integration**: For production, external stages with cloud storage (AWS S3, Azure Blob, GCP) would be used
- **Snowpipe**: For automated ingestion of files from cloud storage
- **Streams**: For incremental processing of changed data
- **Resource Optimization**: Separate warehouses for different workloads
