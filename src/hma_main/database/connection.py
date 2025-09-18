"""Database connection management for RDS MySQL."""
import os
from typing import Optional, Dict, Any, Generator
from contextlib import contextmanager
import pymysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker, Session
from urllib.parse import quote_plus
from dotenv import load_dotenv

from ..core.logging_config import get_logger
from ..core.settings import settings

# Load environment variables
load_dotenv()

logger = get_logger(__name__)


class DatabaseConfig:
    """Database configuration."""
    
    def __init__(self):
        # Load from environment or settings
        self.host = os.getenv('RDS_HOST', 'hma-mysql-db.xxxxx.region.rds.amazonaws.com')
        self.port = int(os.getenv('RDS_PORT', '3306'))
        self.database = os.getenv('RDS_DATABASE', 'hma_data')
        self.username = os.getenv('RDS_USERNAME', 'admin')
        self.password = os.getenv('RDS_PASSWORD', '')
        
        # Connection pool settings
        self.pool_size = 5
        self.max_overflow = 10
        self.pool_recycle = 3600  # Recycle connections after 1 hour
        self.pool_pre_ping = True  # Test connections before using
    
    def get_connection_string(self, use_sqlalchemy: bool = True) -> str:
        """Get database connection string."""
        if use_sqlalchemy:
            # SQLAlchemy format with URL encoding for special characters
            password_encoded = quote_plus(self.password)
            return (
                f"mysql+pymysql://{self.username}:{password_encoded}@"
                f"{self.host}:{self.port}/{self.database}"
                f"?charset=utf8mb4"
            )
        else:
            # PyMySQL format
            return {
                'host': self.host,
                'port': self.port,
                'user': self.username,
                'password': self.password,
                'database': self.database,
                'charset': 'utf8mb4',
                'cursorclass': DictCursor
            }


class DatabaseConnection:
    """Manages database connections with connection pooling."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """Initialize database connection manager."""
        self.config = config or DatabaseConfig()
        self._engine = None
        self._session_factory = None
    
    @property
    def engine(self):
        """Get or create SQLAlchemy engine with connection pooling."""
        if self._engine is None:
            connection_string = self.config.get_connection_string(use_sqlalchemy=True)
            
            self._engine = create_engine(
                connection_string,
                poolclass=pool.QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_recycle=self.config.pool_recycle,
                pool_pre_ping=self.config.pool_pre_ping,
                echo=False  # Set to True for SQL debugging
            )
            
            logger.info("Database engine created successfully")
        
        return self._engine
    
    @property
    def session_factory(self):
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for database sessions."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    @contextmanager
    def get_connection(self):
        """Context manager for raw PyMySQL connections."""
        connection = None
        try:
            connection_params = self.config.get_connection_string(use_sqlalchemy=False)
            connection = pymysql.connect(**connection_params)
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("Database connection test successful")
                    return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> list:
        """Execute a SELECT query and return results."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_update(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
    
    def bulk_insert(self, table: str, data: list, on_duplicate: str = "UPDATE") -> int:
        """
        Bulk insert data into table.
        
        Args:
            table: Table name
            data: List of dictionaries with data
            on_duplicate: Action on duplicate key (UPDATE, IGNORE)
        
        Returns:
            Number of affected rows
        """
        if not data:
            return 0
        
        # Get columns from first row
        columns = list(data[0].keys())
        
        # Build INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'`{col}`' for col in columns])
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        # Add ON DUPLICATE KEY clause
        if on_duplicate == "UPDATE":
            update_clause = ', '.join([f'`{col}` = VALUES(`{col}`)' for col in columns if col != 'id'])
            query += f" ON DUPLICATE KEY UPDATE {update_clause}"
        elif on_duplicate == "IGNORE":
            query = query.replace("INSERT INTO", "INSERT IGNORE INTO")
        
        # Prepare data tuples
        values = [tuple(row.get(col) for col in columns) for row in data]
        
        # Execute bulk insert
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, values)
                affected_rows = cursor.rowcount
                logger.info(f"Bulk inserted {affected_rows} rows into {table}")
                return affected_rows


# Global database connection instance
db = DatabaseConnection()