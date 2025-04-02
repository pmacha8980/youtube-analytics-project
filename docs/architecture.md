# YouTube Analytics Project Architecture

This document outlines the architecture of the YouTube Analytics project.

## Overview

The YouTube Analytics project follows a modern data warehouse architecture using Snowflake as the core platform. It implements an ELT (Extract, Load, Transform) approach to process YouTube video statistics data.

## Architecture Diagram

```
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
|  Source Data     |     |  Snowflake       |     |  Visualization  |
|  (YouTube API,   +---->+  Data Platform   +---->+  (Snowsight,    |
|   CSV, JSON)     |     |                  |     |   ThoughtSpot)  |
|                  |     |                  |     |                  |
+------------------+     +--+----------+----+     +------------------+
                           |          |
                           v          v
                +----------+----+  +--+------------+
                |               |  |               |
                |  Data Quality |  |  Orchestration|
                |  Checks       |  |  (Tasks,      |
                |               |  |   Streams)    |
                +---------------+  +---------------+

+----------------------------------------------------------+
|                                                          |
|                   SNOWFLAKE PLATFORM                     |
|                                                          |
| +----------------+  +----------------+  +----------------+|
| |                |  |                |  |                ||
| | RAW LAYER      |  | STAGING LAYER  |  | DWH LAYER      ||
| | - Raw tables   |  | - Cleansed data|  | - Dim tables   ||
| | - JSON, CSV    |  | - Validated    |  | - Fact tables  ||
| | - External     |  | - Transformed  |  | - Star schema  ||
| |   stage        |  |                |  |                ||
| +----------------+  +----------------+  +----------------+|
|                                                          |
| +----------------+  +----------------+  +----------------+|
| |                |  |                |  |                ||
| | ANALYTICS LAYER|  | DATA QUALITY   |  | METADATA       ||
| | - Views        |  | - DQ checks    |  | - Logging      ||
| | - Aggregations |  | - Monitoring   |  | - Lineage      ||
| | - Metrics      |  | - Alerting     |  | - Versioning   ||
| |                |  |                |  |                ||
| +----------------+  +----------------+  +----------------+|
|                                                          |
+----------------------------------------------------------+

+----------------------------------------------------------+
|                                                          |
|                MULTI-ENVIRONMENT SETUP                   |
|                                                          |
| +----------------+  +----------------+  +----------------+|
| |                |  |                |  |                ||
| | DEV            |  | TEST           |  | PROD           ||
| | Environment    |  | Environment    |  | Environment    ||
| |                |  |                |  |                ||
| +----------------+  +----------------+  +----------------+|
|                                                          |
| +--------------------------------------------------+     |
| |                                                  |     |
| | CI/CD PIPELINE                                   |     |
| | - GitHub Actions                                 |     |
| | - Automated testing                              |     |
| | - Deployment scripts                             |     |
| |                                                  |     |
| +--------------------------------------------------+     |
|                                                          |
+----------------------------------------------------------+
```

## Architecture Components

1. **Data Sources**
   - YouTube API data
   - CSV files with historical data
   - JSON files with supplementary data

2. **Data Ingestion**
   - Snowflake external stages for file loading
   - Snowpipe for automated ingestion
   - Python scripts for API data extraction

3. **Data Storage Layers**
   - Raw Layer: Original data in JSON and CSV format
   - Staging Layer: Parsed and cleansed data
   - Warehouse Layer: Dimensional model (star schema)
   - Analytics Layer: Business views and aggregations

4. **Processing**
   - Snowflake SQL for transformations
   - Stored procedures for complex logic
   - Snowflake tasks for orchestration

5. **Visualization**
   - Snowsight for dashboards and ad-hoc analysis
   - ThoughtSpot for interactive analytics

## Data Flow

1. Data is extracted from YouTube API or loaded from files
2. Raw data is stored in RAW schema tables
3. Data is cleansed and validated in STAGING schema
4. Dimensional model is built in DWH schema
5. Analytics views are created in ANALYTICS schema
6. Dashboards are built on top of analytics views

## Multi-Environment Support

The architecture supports three distinct environments:
- Development (DEV)
- Testing (TEST)
- Production (PROD)

Each environment has:
- Dedicated database
- Role-based access control
- Environment-specific warehouses
- CI/CD pipeline for deployment

## Security

- Role-based access control
- Column-level security for sensitive data
- Secure views for controlled access
- Data encryption at rest and in transit
