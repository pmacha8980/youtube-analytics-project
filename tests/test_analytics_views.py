"""
Tests for YouTube Analytics views
"""
import os
import pytest
import snowflake.connector

@pytest.fixture
def snowflake_conn():
    """Create a connection to Snowflake"""
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE']
    )
    yield conn
    conn.close()

def test_top_videos_by_category_view(snowflake_conn):
    """Test that TOP_VIDEOS_BY_CATEGORY view exists and returns data"""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.TOP_VIDEOS_BY_CATEGORY")
    result = cursor.fetchone()
    assert result[0] > 0, "TOP_VIDEOS_BY_CATEGORY view should return data"
    
    # Test that no category has more than 10 videos
    cursor.execute("""
        SELECT category_name, COUNT(*) as video_count
        FROM ANALYTICS.TOP_VIDEOS_BY_CATEGORY
        GROUP BY category_name
        HAVING COUNT(*) > 10
    """)
    result = cursor.fetchall()
    assert len(result) == 0, "No category should have more than 10 videos"

def test_most_disliked_videos_view(snowflake_conn):
    """Test that MOST_DISLIKED_VIDEOS view exists and returns exactly 5 rows"""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.MOST_DISLIKED_VIDEOS")
    result = cursor.fetchone()
    assert result[0] == 5, "MOST_DISLIKED_VIDEOS view should return exactly 5 rows"
    
    # Test that dislike_percentage is calculated correctly
    cursor.execute("""
        SELECT 
            dislikes, 
            views, 
            dislike_percentage,
            ABS((dislikes / NULLIF(views, 0) * 100) - dislike_percentage) as diff
        FROM ANALYTICS.MOST_DISLIKED_VIDEOS
        WHERE ABS((dislikes / NULLIF(views, 0) * 100) - dislike_percentage) > 0.001
    """)
    result = cursor.fetchall()
    assert len(result) == 0, "dislike_percentage calculation should be correct"

def test_auto_category_likes_view(snowflake_conn):
    """Test that AUTO_CATEGORY_LIKES view exists and returns data for Autos & Vehicles"""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT category_name FROM ANALYTICS.AUTO_CATEGORY_LIKES")
    result = cursor.fetchone()
    assert result[0] == 'Autos & Vehicles', "AUTO_CATEGORY_LIKES should only contain Autos & Vehicles category"
    
    # Test that min_likes <= avg_likes <= max_likes
    cursor.execute("""
        SELECT *
        FROM ANALYTICS.AUTO_CATEGORY_LIKES
        WHERE NOT (min_likes <= avg_likes AND avg_likes <= max_likes)
    """)
    result = cursor.fetchall()
    assert len(result) == 0, "min_likes should be <= avg_likes <= max_likes"

def test_top_categories_videos_2014_2018_view(snowflake_conn):
    """Test that TOP_CATEGORIES_VIDEOS_2014_2018 view exists and returns at most 3 categories"""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.TOP_CATEGORIES_VIDEOS_2014_2018")
    result = cursor.fetchone()
    assert result[0] <= 3, "TOP_CATEGORIES_VIDEOS_2014_2018 view should return at most 3 categories"
    
    # Test that years are within range
    cursor.execute("""
        SELECT *
        FROM ANALYTICS.TOP_CATEGORIES_VIDEOS_2014_2018
        WHERE min_year < 2014 OR max_year > 2018
    """)
    result = cursor.fetchall()
    assert len(result) == 0, "Years should be between 2014 and 2018"

def test_video_counts_by_source_view(snowflake_conn):
    """Test that VIDEO_COUNTS_BY_SOURCE view exists and returns data"""
    cursor = snowflake_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.VIDEO_COUNTS_BY_SOURCE")
    result = cursor.fetchone()
    assert result[0] > 0, "VIDEO_COUNTS_BY_SOURCE view should return data"
    
    # Test that video_count is always positive
    cursor.execute("""
        SELECT *
        FROM ANALYTICS.VIDEO_COUNTS_BY_SOURCE
        WHERE video_count <= 0
    """)
    result = cursor.fetchall()
    assert len(result) == 0, "video_count should always be positive"
