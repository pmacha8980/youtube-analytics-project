#!/usr/bin/env python
"""
Deployment script for YouTube Analytics Project
This script handles deployment to different environments (DEV, TEST, PROD)
"""

import os
import sys
import argparse
import snowflake.connector
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('youtube_analytics_deploy')

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Deploy YouTube Analytics to Snowflake')
    parser.add_argument('--env', required=True, choices=['DEV', 'TEST', 'PROD'],
                        help='Environment to deploy to')
    return parser.parse_args()

def get_snowflake_connection():
    """Create a connection to Snowflake using environment variables"""
    try:
        conn = snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            role=os.environ['SNOWFLAKE_ROLE'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            database=os.environ['SNOWFLAKE_DATABASE']
        )
        logger.info(f"Connected to Snowflake as {os.environ['SNOWFLAKE_USER']}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)

def execute_sql_file(conn, file_path, env):
    """Execute a SQL file with environment variable substitution"""
    try:
        with open(file_path, 'r') as f:
            sql = f.read()
        
        # Replace environment variables
        sql = sql.replace("SET ENV = 'DEV';", f"SET ENV = '{env}';")
        
        # Execute the SQL
        cursor = conn.cursor()
        for statement in sql.split(';'):
            if statement.strip():
                logger.info(f"Executing: {statement[:100]}...")
                cursor.execute(statement)
        cursor.close()
        
        logger.info(f"Successfully executed {file_path}")
    except Exception as e:
        logger.error(f"Error executing {file_path}: {e}")
        raise

def deploy_environment(env):
    """Deploy to the specified environment"""
    logger.info(f"Starting deployment to {env} environment")
    
    # Get project root directory
    project_root = Path(__file__).parent.parent
    
    # Connect to Snowflake
    conn = get_snowflake_connection()
    
    try:
        # Execute setup scripts
        setup_dir = project_root / 'scripts' / 'setup'
        for sql_file in sorted(setup_dir.glob('*.sql')):
            execute_sql_file(conn, sql_file, env)
        
        # Execute ingestion scripts
        ingestion_dir = project_root / 'scripts' / 'ingestion'
        for sql_file in sorted(ingestion_dir.glob('*.sql')):
            execute_sql_file(conn, sql_file, env)
        
        # Execute transformation scripts
        transform_dir = project_root / 'scripts' / 'transformation'
        for sql_file in sorted(transform_dir.glob('*.sql')):
            execute_sql_file(conn, sql_file, env)
        
        # Execute analytics scripts
        analytics_dir = project_root / 'scripts' / 'analytics'
        for sql_file in sorted(analytics_dir.glob('*.sql')):
            if 'parameterized' in sql_file.name:
                execute_sql_file(conn, sql_file, env)
        
        logger.info(f"Deployment to {env} completed successfully")
    except Exception as e:
        logger.error(f"Deployment to {env} failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    args = parse_args()
    deploy_environment(args.env)
