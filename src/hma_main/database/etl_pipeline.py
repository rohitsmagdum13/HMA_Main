"""ETL Pipeline for processing S3 CSV files to RDS MySQL."""
import uuid
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import io
import boto3

from ..core.logging_config import get_logger
from ..core.settings import settings
from ..core.exceptions import UploadError
from ..services.s3_client import build_session
from .connection import db

logger = get_logger(__name__)


class CSVProcessor:
    """Process CSV files from S3 and load to MySQL."""
    
    def __init__(self, aws_profile: Optional[str] = None):
        """Initialize CSV processor."""
        self.session = build_session(
            profile=aws_profile or settings.aws_profile,
            access_key=settings.aws_access_key_id,
            secret_key=settings.aws_secret_access_key,
            region=settings.aws_default_region
        )
        self.s3_client = self.session.client('s3')
        self.job_id = None
    
    def create_job(self, job_type: str, source_file: str) -> str:
        """Create an ETL job record."""
        self.job_id = f"{job_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        query = """
            INSERT INTO etl_jobs (job_id, job_type, source_file, status, started_at)
            VALUES (%(job_id)s, %(job_type)s, %(source_file)s, 'processing', NOW())
        """
        
        db.execute_update(query, {
            'job_id': self.job_id,
            'job_type': job_type,
            'source_file': source_file
        })
        
        logger.info(f"Created ETL job: {self.job_id}")
        return self.job_id
    
    def update_job(self, status: str, records_processed: int = 0, 
                   records_failed: int = 0, error_message: Optional[str] = None):
        """Update ETL job status."""
        query = """
            UPDATE etl_jobs 
            SET status = %(status)s,
                records_processed = %(records_processed)s,
                records_failed = %(records_failed)s,
                error_message = %(error_message)s,
                completed_at = CASE WHEN %(status)s IN ('completed', 'failed') THEN NOW() ELSE NULL END
            WHERE job_id = %(job_id)s
        """
        
        db.execute_update(query, {
            'job_id': self.job_id,
            'status': status,
            'records_processed': records_processed,
            'records_failed': records_failed,
            'error_message': error_message
        })
    
    def log_data_quality(self, check_type: str, table_name: str, 
                        check_result: str, details: Dict):
        """Log data quality check results."""
        query = """
            INSERT INTO data_quality_logs (job_id, check_type, table_name, check_result, details)
            VALUES (%(job_id)s, %(check_type)s, %(table_name)s, %(check_result)s, %(details)s)
        """
        
        import json
        db.execute_update(query, {
            'job_id': self.job_id,
            'check_type': check_type,
            'table_name': table_name,
            'check_result': check_result,
            'details': json.dumps(details)
        })
    
    def download_csv_from_s3(self, bucket: str, key: str) -> pd.DataFrame:
        """Download CSV from S3 and return as DataFrame."""
        try:
            logger.info(f"Downloading CSV from s3://{bucket}/{key}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            
            # Read CSV content
            csv_content = response['Body'].read().decode('utf-8')
            
            # Create DataFrame
            df = pd.read_csv(io.StringIO(csv_content))
            
            logger.info(f"Downloaded CSV with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error downloading CSV from S3: {e}")
            raise
    
    def validate_dataframe(self, df: pd.DataFrame, table_name: str) -> Tuple[bool, Dict]:
        """Validate DataFrame before loading to database."""
        validation_results = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'null_counts': {},
            'duplicate_rows': 0,
            'issues': []
        }
        
        # Check for null values
        null_counts = df.isnull().sum()
        validation_results['null_counts'] = null_counts.to_dict()
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        validation_results['duplicate_rows'] = duplicate_count
        
        # Specific validation based on table
        is_valid = True
        
        if table_name == 'member_data':
            # Check required fields
            if 'member_id' not in df.columns:
                validation_results['issues'].append("Missing member_id column")
                is_valid = False
            elif df['member_id'].isnull().any():
                validation_results['issues'].append("Null values in member_id")
                is_valid = False
        
        elif table_name == 'deductibles_oop':
            # Check for metric column
            if 'Metric' not in df.columns:
                validation_results['issues'].append("Missing Metric column")
                is_valid = False
        
        # Log data quality check
        check_result = 'pass' if is_valid else 'fail'
        self.log_data_quality(
            check_type='dataframe_validation',
            table_name=table_name,
            check_result=check_result,
            details=validation_results
        )
        
        return is_valid, validation_results
    
    def transform_member_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform member data DataFrame."""
        # Ensure column names match database schema
        df.columns = df.columns.str.lower()
        
        # Convert date columns
        if 'dob' in df.columns:
            df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
        
        # Clean string columns
        string_columns = ['member_id', 'first_name', 'last_name']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        return df
    
    def transform_deductibles_oop(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform deductibles OOP DataFrame."""
        # Rename columns to match database
        df.columns = df.columns.str.lower()
        if 'metric' in df.columns:
            df.rename(columns={'metric': 'metric'}, inplace=True)
        
        # Convert numeric columns
        numeric_columns = ['m1001', 'm1002', 'm1003', 'm1004', 'm1005']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Add member_id mapping if needed
        # This is a simplified example - you might need more complex logic
        if 'member_id' not in df.columns:
            # Extract member_id from metric if it contains it
            df['member_id'] = df['metric'].str.extract(r'(M\d{4})', expand=False)
        
        return df
    
    def transform_benefit_accumulator(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform benefit accumulator DataFrame."""
        df.columns = df.columns.str.lower()
        
        # Convert numeric columns
        numeric_columns = ['used', 'remaining']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        return df
    
    def transform_plan_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform plan details DataFrame."""
        df.columns = df.columns.str.lower()
        
        # Convert numeric columns
        if 'group_number' in df.columns:
            df['group_number'] = pd.to_numeric(df['group_number'], errors='coerce')
        if 'plan_detail' in df.columns:
            df['plan_detail'] = pd.to_numeric(df['plan_detail'], errors='coerce')
        
        return df
    
    def load_to_mysql(self, df: pd.DataFrame, table_name: str) -> int:
        """Load DataFrame to MySQL table."""
        try:
            # Convert DataFrame to list of dictionaries
            data = df.to_dict('records')
            
            # Bulk insert
            affected_rows = db.bulk_insert(table_name, data, on_duplicate="UPDATE")
            
            logger.info(f"Loaded {affected_rows} rows to {table_name}")
            return affected_rows
            
        except Exception as e:
            logger.error(f"Error loading data to MySQL: {e}")
            raise
    
    def process_csv_file(self, bucket: str, key: str, table_name: str) -> Dict:
        """
        Process a single CSV file from S3 to MySQL.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            table_name: Target MySQL table name
        
        Returns:
            Processing results
        """
        results = {
            'success': False,
            'job_id': None,
            'records_processed': 0,
            'records_failed': 0,
            'error': None
        }
        
        try:
            # Create ETL job
            self.create_job(
                job_type=f"csv_to_{table_name}",
                source_file=f"s3://{bucket}/{key}"
            )
            results['job_id'] = self.job_id
            
            # Download CSV from S3
            df = self.download_csv_from_s3(bucket, key)
            
            # Validate DataFrame
            is_valid, validation_results = self.validate_dataframe(df, table_name)
            
            if not is_valid:
                raise ValueError(f"Validation failed: {validation_results['issues']}")
            
            # Transform based on table type
            if table_name == 'member_data':
                df = self.transform_member_data(df)
            elif table_name == 'deductibles_oop':
                df = self.transform_deductibles_oop(df)
            elif table_name == 'benefit_accumulator':
                df = self.transform_benefit_accumulator(df)
            elif table_name == 'plan_details':
                df = self.transform_plan_details(df)
            
            # Load to MySQL
            affected_rows = self.load_to_mysql(df, table_name)
            
            # Update job status
            self.update_job(
                status='completed',
                records_processed=affected_rows,
                records_failed=0
            )
            
            results['success'] = True
            results['records_processed'] = affected_rows
            
            logger.info(f"Successfully processed {key} to {table_name}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing CSV file: {error_msg}")
            
            # Update job status
            if self.job_id:
                self.update_job(
                    status='failed',
                    records_processed=0,
                    records_failed=0,
                    error_message=error_msg
                )
            
            results['error'] = error_msg
        
        return results
    
    def process_all_mba_csvs(self) -> Dict:
        """Process all CSV files in MBA bucket."""
        bucket = settings.s3_bucket_mba
        results = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        try:
            # List all CSV files in bucket
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=bucket,
                Prefix='mba/csv/'
            )
            
            # File to table mapping
            file_table_mapping = {
                'MemberData': 'member_data',
                'memberdata': 'member_data',
                'deductibles_oop': 'deductibles_oop',
                'benefit_accumulator': 'benefit_accumulator',
                'plan_details': 'plan_details'
            }
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # Skip non-CSV files
                        if not key.endswith('.csv'):
                            continue
                        
                        results['total_files'] += 1
                        
                        # Determine target table based on filename
                        filename = Path(key).stem.lower()
                        table_name = None
                        
                        for pattern, table in file_table_mapping.items():
                            if pattern.lower() in filename:
                                table_name = table
                                break
                        
                        if not table_name:
                            logger.warning(f"Unknown file type: {key}")
                            results['failed'] += 1
                            results['details'].append({
                                'file': key,
                                'status': 'failed',
                                'error': 'Unknown file type'
                            })
                            continue
                        
                        # Process the CSV file
                        logger.info(f"Processing {key} to {table_name}")
                        process_result = self.process_csv_file(bucket, key, table_name)
                        
                        if process_result['success']:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        
                        results['details'].append({
                            'file': key,
                            'table': table_name,
                            'status': 'success' if process_result['success'] else 'failed',
                            'records': process_result['records_processed'],
                            'job_id': process_result['job_id'],
                            'error': process_result.get('error')
                        })
            
            logger.info(f"Processed {results['successful']}/{results['total_files']} files successfully")
            
        except Exception as e:
            logger.error(f"Error processing MBA CSVs: {e}")
            results['error'] = str(e)
        
        return results


class DataQualityChecker:
    """Perform data quality checks on loaded data."""
    
    def check_referential_integrity(self) -> Dict:
        """Check referential integrity between tables."""
        results = {}
        
        # Check orphaned records in deductibles_oop
        query = """
            SELECT COUNT(*) as orphaned_count
            FROM deductibles_oop d
            LEFT JOIN member_data m ON d.member_id = m.member_id
            WHERE m.member_id IS NULL AND d.member_id IS NOT NULL
        """
        
        result = db.execute_query(query)
        results['deductibles_oop_orphans'] = result[0]['orphaned_count'] if result else 0
        
        # Check orphaned records in benefit_accumulator
        query = """
            SELECT COUNT(*) as orphaned_count
            FROM benefit_accumulator b
            LEFT JOIN member_data m ON b.member_id = m.member_id
            WHERE m.member_id IS NULL
        """
        
        result = db.execute_query(query)
        results['benefit_accumulator_orphans'] = result[0]['orphaned_count'] if result else 0
        
        return results
    
    def check_data_completeness(self) -> Dict:
        """Check data completeness in tables."""
        results = {}
        
        # Check member_data completeness
        query = """
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) as null_first_name,
                SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) as null_last_name,
                SUM(CASE WHEN dob IS NULL THEN 1 ELSE 0 END) as null_dob
            FROM member_data
        """
        
        result = db.execute_query(query)
        results['member_data'] = result[0] if result else {}
        
        return results
    
    def generate_summary_report(self) -> Dict:
        """Generate summary report of data in database."""
        report = {}
        
        # Get record counts for each table
        tables = ['member_data', 'deductibles_oop', 'benefit_accumulator', 'plan_details']
        
        for table in tables:
            query = f"SELECT COUNT(*) as count FROM {table}"
            result = db.execute_query(query)
            report[f'{table}_count'] = result[0]['count'] if result else 0
        
        # Get recent ETL jobs
        query = """
            SELECT job_id, job_type, status, records_processed, started_at
            FROM etl_jobs
            ORDER BY created_at DESC
            LIMIT 10
        """
        
        report['recent_jobs'] = db.execute_query(query)
        
        # Get data quality summary
        query = """
            SELECT 
                check_result,
                COUNT(*) as count
            FROM data_quality_logs
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY check_result
        """
        
        report['quality_summary'] = db.execute_query(query)
        
        return report