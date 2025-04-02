# YouTube Analytics Project

This project implements an end-to-end data solution for YouTube video statistics using Snowflake.

## Project Overview

This project creates a complete data pipeline for analyzing YouTube video statistics. It follows an ELT approach with:

- Raw data ingestion from CSV and JSON files
- Transformation into a star schema dimensional model
- Analytics views for business insights
- Orchestration for automated processing
- Data quality checks

## Project Structure

```
youtube_analytics_project/
├── scripts/                 # Snowflake SQL scripts
│   ├── setup/               # Initial setup scripts
│   ├── ingestion/           # Data loading scripts
│   ├── transformation/      # Data modeling scripts
│   ├── analytics/           # Analytics views
│   ├── orchestration/       # Pipeline orchestration scripts
│   └── data_quality/        # Data quality checks
├── docs/                    # Documentation and diagrams
│   ├── architecture.md      # Architecture overview
│   └── multi_environment_setup.md  # Multi-environment setup
└── data/                    # Sample data and utilities
    └── sample_data_load.py  # Python script for loading data
```

## Star Schema Design

![Star Schema](docs/star_schema.png)

- **Fact Table**: fact_video_stats (views, likes, dislikes, comments)
- **Dimension Tables**: dim_video, dim_category, dim_date

## Implementation Steps

1. Set up Snowflake environment (database, schemas, warehouses)
2. Load data from Kaggle into Snowflake raw tables
3. Transform data into staging tables with data cleaning
4. Create dimension and fact tables in star schema
5. Build analytics views for insights
6. Set up orchestration with tasks and streams
7. Implement data quality checks

## Requirements

- Snowflake account
- YouTube Video Statistics dataset from Kaggle
- Python 3.7+ (for helper scripts)
- Snowflake Connector for Python
- Kaggle API (for downloading dataset)

## Getting Started

1. Create a Snowflake trial account if you don't have one
2. Run the setup scripts to create Snowflake objects:
   ```
   snowsql -f scripts/setup/01_create_environment.sql
   ```
3. Download and load the YouTube dataset:
   ```
   python data/sample_data_load.py
   ```
4. Run the transformation scripts in sequence:
   ```
   snowsql -f scripts/transformation/01_create_staging_tables.sql
   snowsql -f scripts/transformation/02_create_dimension_tables.sql
   snowsql -f scripts/transformation/03_create_fact_table.sql
   ```
5. Create analytics views:
   ```
   snowsql -f scripts/analytics/01_create_analytics_views.sql
   ```
6. Set up orchestration:
   ```
   snowsql -f scripts/orchestration/01_setup_orchestration.sql
   ```
7. Open Snowsight to create visualizations using the analytics views

## Visualizations

The project includes several analytics views that can be used to create visualizations in Snowsight:

1. TOP_VIDEOS_BY_CATEGORY - Top 10 viewed videos by each category
2. MOST_DISLIKED_VIDEOS - 5 overall most disliked videos
3. AUTO_CATEGORY_LIKES - Average likes in 'Autos & Vehicles' category
4. TOP_CATEGORIES_VIDEOS_2014_2018 - Videos in top categories by likes (2014-2018)
5. VIDEO_COUNTS_BY_SOURCE - Video counts by file name and load date

## Bonus Features

1. **Data Quality Checks**: Implemented in scripts/data_quality/01_data_quality_checks.sql
2. **Multi-Environment Support**: Documentation in docs/multi_environment_setup.md
