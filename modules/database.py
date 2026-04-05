"""
Database Module
Handles in-memory SQL database operations using SQLite
"""

import sqlite3
import pandas as pd
from typing import Optional, List, Dict, Any


class DatabaseEngine:
    """
    Manages in-memory SQLite database for query execution
    Provides schema extraction and safe query execution
    """

    def __init__(self):
        """Initialize in-memory SQLite database"""
        self.connection = sqlite3.connect(":memory:")
        self.cursor = self.connection.cursor()

    def load_dataframe(self, df: pd.DataFrame, table_name: str = "data") -> None:
        """
        Load pandas DataFrame into SQLite

        Args:
            df (pd.DataFrame): DataFrame to load
            table_name (str): Name of the table to create
        """
        df.to_sql(table_name, self.connection, if_exists="replace", index=False)

    def get_schema(self) -> str:
        """
        Extract schema from loaded tables

        Returns:
            str: CREATE TABLE statement showing schema
        """
        try:
            self.cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='data'"
            )
            result = self.cursor.fetchone()
            if result:
                return result[0]
            return ""
        except Exception as e:
            raise Exception(f"Error extracting schema: {str(e)}")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query safely

        Args:
            query (str): SQL query to execute

        Returns:
            pd.DataFrame: Query results as DataFrame

        Raises:
            Exception: If query execution fails
        """
        try:
            # Validate query doesn't contain dangerous operations
            forbidden_keywords = ["DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE"]
            query_upper = query.upper()

            for keyword in forbidden_keywords:
                if keyword in query_upper:
                    raise ValueError(
                        f"Query contains forbidden operation: {keyword}. Read-only queries only."
                    )

            # Execute query
            result_df = pd.read_sql_query(query, self.connection)
            return result_df

        except ValueError as ve:
            raise ValueError(f"Invalid query: {str(ve)}")
        except sqlite3.Error as se:
            raise Exception(f"SQL Error: {str(se)}")
        except Exception as e:
            raise Exception(f"Query execution error: {str(e)}")

    def get_table_info(self, table_name: str = "data") -> Dict[str, Any]:
        """
        Get detailed information about table

        Args:
            table_name (str): Name of table

        Returns:
            Dict: Table metadata
        """
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()

            return {
                "table_name": table_name,
                "columns": [
                    {
                        "name": col[1],
                        "type": col[2],
                        "nullable": col[3],
                        "default": col[4],
                    }
                    for col in columns
                ],
            }
        except Exception as e:
            raise Exception(f"Error getting table info: {str(e)}")

    def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
