import os
import psycopg2
from psycopg2 import pool
from typing import Any, Dict, List, Tuple, Optional, Union

class PostgresManager:
    """
    A class for managing PostgreSQL database connections and operations.
    """

    def __init__(
        self,
        host: str = None,
        port: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        min_connections: int = 1,
        max_connections: int = 10
    ):
        """
        Initialize the PostgresManager with connection parameters.
        """
        # Use parameters or environment variables
        self.host = host or os.environ.get("DB_HOST", "localhost")
        self.port = port or os.environ.get("DB_PORT", "5432")
        self.database = database or os.environ.get("DB_NAME")
        self.user = user or os.environ.get("DB_USER")
        self.password = password or os.environ.get("DB_PASSWORD")

        # Validate that required connection info is provided
        if not all([self.database, self.user, self.password]):
            raise ValueError("Database name, user, and password must be provided.")

        # Initialize the connection pool
        self.connection_pool = None
        self.min_connections = min_connections
        self.max_connections = max_connections

    def initialize_pool(self) -> None:
        if self.connection_pool is None:
            try:
                self.connection_pool = pool.ThreadedConnectionPool(
                    self.min_connections,
                    self.max_connections,
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                print(f"Connection pool initialized with {self.min_connections}-{self.max_connections} connections")
            except psycopg2.Error as e:
                raise ConnectionError(f"Failed to initialize connection pool: {e}")

    def get_connection(self) -> Tuple[Any, Any]:
        """
        Returns:
            Tuple containing (connection, cursor)
        """
        if self.connection_pool is None:
            self.initialize_pool()

        try:
            connection = self.connection_pool.getconn()
            cursor = connection.cursor()
            return connection, cursor
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to get connection from pool: {e}")

    def release_connection(self, connection: Any) -> None:
        """
            connection: The connection to release back to the pool
        """
        if self.connection_pool is not None:
            self.connection_pool.putconn(connection)

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple, Dict]] = None,
        fetchone: bool = False,
        fetchall: bool = True
    ) -> Optional[Union[Tuple, List[Tuple]]]:
        """
        Execute a SQL query and optionally return the results.
        """
        connection = None
        try:
            connection, cursor = self.get_connection()
            cursor.execute(query, params)

            if query.strip().upper().startswith("SELECT"):
                if fetchone:
                    result = cursor.fetchone()
                elif fetchall:
                    result = cursor.fetchall()
                else:
                    result = None
                connection.commit()
                return result
            else:
                connection.commit()
                return None

        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            raise DatabaseError(f"Query execution failed: {e}")
        finally:
            if connection:
                self.release_connection(connection)

    def execute_batch(self, query: str, params_list: List[Union[Tuple, Dict]]) -> None:
        """
        Execute a batch of queries with different parameters.
        """
        connection = None
        try:
            connection, cursor = self.get_connection()

            for params in params_list:
                cursor.execute(query, params)

            connection.commit()
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            raise DatabaseError(f"Batch execution failed: {e}")
        finally:
            if connection:
                self.release_connection(connection)

    def close_pool(self) -> None:
        if self.connection_pool is not None:
            self.connection_pool.closeall()
            self.connection_pool = None
            print("Connection pool closed")

    def __enter__(self):
        self.initialize_pool()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_pool()

class DatabaseError(Exception):
    """Exception raised for database operation errors."""
    pass
