# YouTube Analytics Dashboard Setup Guide

This guide provides instructions for setting up a dashboard for the YouTube Analytics project using Snowflake Partner Connect.

## Accessing Partner Connect

1. Log into your Snowflake account
2. Navigate to Admin > Partner Connect
3. Choose a BI tool from the available options:
   - Tableau
   - Power BI
   - Looker
   - ThoughtSpot
   - Sigma

## Setting Up the Connection

1. Click on your chosen BI tool
2. Follow the prompts to create a connection
3. Snowflake will automatically create:
   - A service account
   - A warehouse for the BI tool
   - Necessary grants and permissions

## Dashboard Design

Create a dashboard with the following components:

### 1. Category Performance Dashboard

**Primary View**: `TOP_VIDEOS_BY_CATEGORY`

**Visualizations**:
- Bar chart showing top videos by views for each category
- Filters for selecting specific categories
- Table view with detailed video information

**Sample Query**:
```sql
SELECT 
  category_name,
  title,
  channel_title,
  views
FROM ANALYTICS.TOP_VIDEOS_BY_CATEGORY
ORDER BY category_name, views DESC;
```

### 2. Video Engagement Dashboard

**Primary Views**: `MOST_DISLIKED_VIDEOS`, `AUTO_CATEGORY_LIKES`

**Visualizations**:
- Bar chart showing most disliked videos
- Scatter plot of views vs. dislikes
- Summary cards showing average, min, and max likes for Auto category

**Sample Query**:
```sql
SELECT 
  title,
  channel_title,
  category_name,
  views,
  dislikes,
  dislike_percentage
FROM ANALYTICS.MOST_DISLIKED_VIDEOS
ORDER BY dislikes DESC;
```

### 3. Historical Trends Dashboard

**Primary View**: `TOP_CATEGORIES_VIDEOS_2014_2018`

**Visualizations**:
- Line chart showing trends over years 2014-2018
- Bar chart comparing total likes across top categories
- Table showing video counts by category and year

**Sample Query**:
```sql
SELECT 
  category_name,
  total_likes,
  unique_video_count,
  min_year,
  max_year
FROM ANALYTICS.TOP_CATEGORIES_VIDEOS_2014_2018
ORDER BY total_likes DESC;
```

### 4. Data Quality Dashboard

**Primary View**: `VIDEO_COUNTS_BY_SOURCE`

**Visualizations**:
- Time series chart showing video counts by load date
- Bar chart showing video counts by source file
- Summary metrics on total videos loaded

**Sample Query**:
```sql
SELECT 
  source_file,
  load_date,
  video_count
FROM ANALYTICS.VIDEO_COUNTS_BY_SOURCE
ORDER BY load_date, source_file;
```

## Environment-Specific Dashboards

For multi-environment support, create separate dashboard connections for each environment:

1. **Development Dashboard**:
   - Connect to `YOUTUBE_ANALYTICS_DEV` database
   - Use `YOUTUBE_DEV_WH` warehouse
   - Label dashboard as "YouTube Analytics - DEV"

2. **Testing Dashboard**:
   - Connect to `YOUTUBE_ANALYTICS_TEST` database
   - Use `YOUTUBE_TEST_WH` warehouse
   - Label dashboard as "YouTube Analytics - TEST"

3. **Production Dashboard**:
   - Connect to `YOUTUBE_ANALYTICS_PROD` database
   - Use `YOUTUBE_PROD_WH` warehouse
   - Label dashboard as "YouTube Analytics - PROD"

## Dashboard Access Control

Configure access control for your dashboards:

1. **Development**: Grant access to development team members
2. **Testing**: Grant access to QA team and stakeholders for review
3. **Production**: Grant access to business users and analysts

## Refresh Schedule

Set up automatic refresh schedules:

1. **Development**: Refresh on demand
2. **Testing**: Refresh daily
3. **Production**: Refresh hourly during business hours
