"""CLI for database operations."""
import argparse
import sys
from pathlib import Path
from tabulate import tabulate

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.hma_main.core.logging_config import get_logger, setup_root_logger
from src.hma_main.database.connection import db, DatabaseConfig
from src.hma_main.database.etl_pipeline import CSVProcessor, DataQualityChecker

logger = get_logger(__name__)


def setup_database():
    """Set up database schema."""
    try:
        logger.info("Setting up database schema...")
        
        # Read schema file
        schema_file = Path(__file__).parent.parent.parent.parent / "database" / "schema.sql"
        
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        # Execute schema
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Split by semicolon and execute each statement
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                
                for statement in statements:
                    if statement:
                        cursor.execute(statement)
                        logger.debug(f"Executed: {statement[:50]}...")
        
        logger.info("Database schema setup completed")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False


def process_s3_csvs(bucket_type: str = 'mba'):
    """Process CSV files from S3 to database."""
    try:
        processor = CSVProcessor()
        
        if bucket_type == 'mba':
            logger.info("Processing MBA bucket CSV files...")
            results = processor.process_all_mba_csvs()
        else:
            logger.error(f"Unsupported bucket type: {bucket_type}")
            return
        
        # Display results
        print("\n" + "="*50)
        print("ETL Processing Results")
        print("="*50)
        print(f"Total Files: {results.get('total_files', 0)}")
        print(f"Successful: {results.get('successful', 0)}")
        print(f"Failed: {results.get('failed', 0)}")
        
        if results.get('details'):
            print("\nFile Details:")
            headers = ['File', 'Table', 'Status', 'Records', 'Job ID']
            table_data = []
            
            for detail in results['details']:
                table_data.append([
                    Path(detail['file']).name,
                    detail.get('table', 'N/A'),
                    detail['status'],
                    detail.get('records', 0),
                    detail.get('job_id', 'N/A')[:20]
                ])
            
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        
    except Exception as e:
        logger.error(f"Error processing S3 CSVs: {e}")
        sys.exit(1)


def check_data_quality():
    """Run data quality checks."""
    try:
        checker = DataQualityChecker()
        
        logger.info("Running data quality checks...")
        
        # Check referential integrity
        ref_integrity = checker.check_referential_integrity()
        
        print("\n" + "="*50)
        print("Referential Integrity Check")
        print("="*50)
        for key, value in ref_integrity.items():
            print(f"{key}: {value} orphaned records")
        
        # Check data completeness
        completeness = checker.check_data_completeness()
        
        print("\n" + "="*50)
        print("Data Completeness Check")
        print("="*50)
        for table, stats in completeness.items():
            if isinstance(stats, dict):
                print(f"\n{table}:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"Error checking data quality: {e}")
        sys.exit(1)


def generate_report():
    """Generate database summary report."""
    try:
        checker = DataQualityChecker()
        report = checker.generate_summary_report()
        
        print("\n" + "="*50)
        print("Database Summary Report")
        print("="*50)
        
        # Table counts
        print("\nTable Record Counts:")
        for key, value in report.items():
            if key.endswith('_count'):
                table_name = key.replace('_count', '')
                print(f"  {table_name}: {value:,} records")
        
        # Recent jobs
        if report.get('recent_jobs'):
            print("\nRecent ETL Jobs:")
            headers = ['Job ID', 'Type', 'Status', 'Records', 'Started At']
            table_data = []
            
            for job in report['recent_jobs']:
                table_data.append([
                    job['job_id'][:30],
                    job['job_type'],
                    job['status'],
                    job.get('records_processed', 0),
                    str(job.get('started_at', 'N/A'))
                ])
            
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        
        # Quality summary
        if report.get('quality_summary'):
            print("\nData Quality Summary (Last 24 Hours):")
            for item in report['quality_summary']:
                print(f"  {item['check_result']}: {item['count']} checks")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    setup_root_logger()
    
    parser = argparse.ArgumentParser(
        description="HMA Database ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set up database schema
  python -m hma_ingestion.database.cli setup
  
  # Process S3 CSV files to database
  python -m hma_ingestion.database.cli process --bucket mba
  
  # Check data quality
  python -m hma_ingestion.database.cli quality
  
  # Generate summary report
  python -m hma_ingestion.database.cli report
  
  # Test database connection
  python -m hma_ingestion.database.cli test
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up database schema')
    
    # Process command
    process_parser = subparsers.add_parser('process', help='Process S3 CSV files')
    process_parser.add_argument(
        '--bucket',
        choices=['mba', 'policy'],
        default='mba',
        help='S3 bucket type to process'
    )
    
    # Quality command
    quality_parser = subparsers.add_parser('quality', help='Run data quality checks')
    
    # Report command
    report_parser = subparsers.add_parser('report', help='Generate summary report')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Test database connection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'setup':
        if setup_database():
            print("Database setup completed successfully")
        else:
            print("Database setup failed")
            sys.exit(1)
    
    elif args.command == 'process':
        process_s3_csvs(args.bucket)
    
    elif args.command == 'quality':
        check_data_quality()
    
    elif args.command == 'report':
        generate_report()
    
    elif args.command == 'test':
        if db.test_connection():
            print("Database connection successful")
            
            # Show configuration
            config = DatabaseConfig()
            print(f"\nConnection Details:")
            print(f"  Host: {config.host}")
            print(f"  Port: {config.port}")
            print(f"  Database: {config.database}")
            print(f"  Username: {config.username}")
        else:
            print("Database connection failed")
            sys.exit(1)


if __name__ == "__main__":
    main()
