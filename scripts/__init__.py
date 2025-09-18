"""Monitor ETL pipeline status."""
import boto3
import time
from datetime import datetime, timedelta
from hma_main.database.connection import db

def monitor_jobs():
    """Monitor ETL jobs in real-time."""
    while True:
        # Get recent jobs
        query = """
            SELECT job_id, job_type, status, records_processed, 
                   started_at, completed_at
            FROM etl_jobs
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT 10
        """
        
        one_hour_ago = datetime.now() - timedelta(hours=1)
        results = db.execute_query(query, (one_hour_ago,))
        
        # Clear screen and display
        print("\033[H\033[J")  # Clear screen
        print("="*80)
        print(f"ETL Pipeline Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        for job in results:
            status_emoji = {
                'completed': '✅',
                'processing': '⏳',
                'failed': '❌',
                'pending': '⏸️'
            }.get(job['status'], '❓')
            
            print(f"{status_emoji} {job['job_id'][:30]}")
            print(f"   Type: {job['job_type']}, Records: {job['records_processed']}")
            print(f"   Started: {job['started_at']}, Completed: {job['completed_at']}")
            print("-"*40)
        
        time.sleep(5)  # Refresh every 5 seconds

if __name__ == "__main__":
    monitor_jobs()