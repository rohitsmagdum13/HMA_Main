"""Database CLI commands."""
import click
import os
from pathlib import Path
from dotenv import load_dotenv
from .connection import db

# Load environment variables from project root
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)


@click.group()
def main():
    """Database management commands."""
    pass


@main.command()
def test():
    """Test database connection."""
    if db.test_connection():
        click.echo("✅ Database connection successful")
    else:
        click.echo("❌ Database connection failed")
        exit(1)


@main.command()
def debug():
    """Debug environment variables."""
    import os
    click.echo(f"ENV file path: {env_path}")
    click.echo(f"ENV file exists: {env_path.exists()}")
    click.echo(f"AWS_ACCESS_KEY_ID: {'Found' if os.getenv('AWS_ACCESS_KEY_ID') else 'Missing'}")
    click.echo(f"AWS_SECRET_ACCESS_KEY: {'Found' if os.getenv('AWS_SECRET_ACCESS_KEY') else 'Missing'}")
    click.echo(f"AWS_DEFAULT_REGION: {os.getenv('AWS_DEFAULT_REGION', 'Not set')}")


@main.command()
def setup():
    """Setup database tables."""
    try:
        # Create basic tables
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS file_uploads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_path VARCHAR(500) NOT NULL,
            s3_key VARCHAR(500) NOT NULL,
            file_size BIGINT,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) DEFAULT 'uploaded'
        );
        """
        
        db.execute_update(create_tables_sql)
        click.echo("✅ Database tables created successfully")
    except Exception as e:
        click.echo(f"❌ Database setup failed: {e}")
        exit(1)


@main.command()
def report():
    """Generate database report."""
    try:
        stats_query = """
        SELECT 
            COUNT(*) as total_files,
            SUM(file_size) as total_size,
            AVG(file_size) as avg_size,
            MIN(upload_date) as first_upload,
            MAX(upload_date) as last_upload
        FROM file_uploads
        """
        
        stats = db.execute_query(stats_query)
        if stats:
            stat = stats[0]
            click.echo("📊 Database Report")
            click.echo(f"Total Files: {stat['total_files']}")
            click.echo(f"Total Size: {stat['total_size']:,} bytes")
            click.echo(f"Average Size: {stat['avg_size']:,.0f} bytes")
            click.echo(f"First Upload: {stat['first_upload']}")
            click.echo(f"Last Upload: {stat['last_upload']}")
        else:
            click.echo("No data found in database")
            
    except Exception as e:
        click.echo(f"❌ Report generation failed: {e}")
        exit(1)


@main.command()
@click.option('--bucket', required=True, help='S3 bucket to process (mba or policy)')
def process(bucket):
    """Process S3 bucket data into database."""
    import boto3
    import os
    
    try:
        # Map bucket names
        bucket_map = {
            'mba': 'hma-mba-bucket',
            'policy': 'hma-policy-bucket'
        }
        
        if bucket not in bucket_map:
            click.echo(f"❌ Invalid bucket: {bucket}. Use 'mba' or 'policy'")
            exit(1)
        
        bucket_name = bucket_map[bucket]
        
        # Debug: Check if credentials are loaded
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        if not access_key or not secret_key:
            click.echo(f"❌ AWS credentials not found. Check .env file")
            click.echo(f"Access Key: {'Found' if access_key else 'Missing'}")
            click.echo(f"Secret Key: {'Found' if secret_key else 'Missing'}")
            exit(1)
        
        # Clear AWS_PROFILE to avoid profile conflicts
        if 'AWS_PROFILE' in os.environ:
            del os.environ['AWS_PROFILE']
        
        # Configure S3 client with explicit credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        
        click.echo(f"Processing S3 bucket: {bucket_name}")
        
        # List objects in bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            click.echo("No files found in bucket")
            return
        
        files_data = []
        for obj in response['Contents']:
            files_data.append({
                'file_path': obj['Key'],
                's3_key': obj['Key'],
                'file_size': obj['Size']
            })
        
        # Insert into database
        affected_rows = db.bulk_insert('file_uploads', files_data, on_duplicate='UPDATE')
        click.echo(f"✅ Processed {len(files_data)} files, {affected_rows} rows affected")
        
    except Exception as e:
        click.echo(f"❌ Processing failed: {e}")
        exit(1)


@main.command()
def quality():
    """Run data quality checks."""
    try:
        # Basic quality checks
        total_files = db.execute_query("SELECT COUNT(*) as count FROM file_uploads")[0]['count']
        duplicate_s3_keys = db.execute_query(
            "SELECT COUNT(*) as count FROM (SELECT s3_key FROM file_uploads GROUP BY s3_key HAVING COUNT(*) > 1) as dups"
        )[0]['count']
        zero_size_files = db.execute_query("SELECT COUNT(*) as count FROM file_uploads WHERE file_size = 0")[0]['count']
        
        click.echo(f"📊 Data Quality Report:")
        click.echo(f"Total files: {total_files}")
        click.echo(f"Duplicate S3 keys: {duplicate_s3_keys}")
        click.echo(f"Zero-size files: {zero_size_files}")
        
        if duplicate_s3_keys > 0 or zero_size_files > 0:
            click.echo("⚠️  Quality issues detected")
        else:
            click.echo("✅ No quality issues found")
            
    except Exception as e:
        click.echo(f"❌ Quality check failed: {e}")
        exit(1)


@main.command()
@click.argument('sql')
def query(sql):
    """Execute SQL query."""
    try:
        results = db.execute_query(sql)
        if results:
            for row in results:
                click.echo(row)
        else:
            click.echo("No results")
    except Exception as e:
        click.echo(f"❌ Query failed: {e}")
        exit(1)


@main.command()
def create_tables():
    """Create additional data tables."""
    try:
        member_data_sql = """
        CREATE TABLE IF NOT EXISTS member_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            member_id VARCHAR(50),
            name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        db.execute_update(member_data_sql)
        click.echo("✅ member_data table created")
        
    except Exception as e:
        click.echo(f"❌ Table creation failed: {e}")
        exit(1)


if __name__ == '__main__':
    main()