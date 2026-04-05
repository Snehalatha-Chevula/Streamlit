"""
Data Processor Module
Handles result formatting and aggregation for visualization
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional


class DataProcessor:
    """
    Processes query results for optimal visualization
    Handles aggregations, filtering, and data transformation
    """

    def __init__(self):
        """Initialize data processor"""
        pass

    def process_results(
        self, df: pd.DataFrame, chart_type: str, axis_mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Process query results for visualization

        Args:
            df (pd.DataFrame): Raw query results
            chart_type (str): Type of chart to render
            axis_mapping (Dict): Mapping of axes to columns

        Returns:
            pd.DataFrame: Processed data ready for visualization
        """

        if df.empty:
            raise ValueError("Query returned no results")

        # Convert data types
        df = self._convert_dtypes(df)

        # Handle missing values
        df = self._handle_missing_values(df)

        # Sort data appropriately
        if "x_axis" in axis_mapping and axis_mapping["x_axis"] in df.columns:
            try:
                df = df.sort_values(by=axis_mapping["x_axis"])
            except Exception:
                pass

        # Limit rows for large datasets
        if len(df) > 500:
            df = df.head(500)

        return df

    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert data types appropriately

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with converted types
        """

        for col in df.columns:
            # Try to convert to numeric if possible
            if df[col].dtype == "object":
                try:
                    df[col] = pd.to_numeric(df[col])
                except Exception:
                    pass

            # Try to convert to datetime
            if df[col].dtype == "object":
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception:
                    pass

        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in data

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with handled missing values
        """

        # Fill numeric columns with 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        # Fill string columns with "Unknown"
        string_cols = df.select_dtypes(include=["object"]).columns
        df[string_cols] = df[string_cols].fillna("Unknown")

        # Drop completely empty rows
        df = df.dropna(how="all")

        return df

    def aggregate_data(
        self, df: pd.DataFrame, group_by: List[str], agg_columns: List[str], agg_func: str = "sum"
    ) -> pd.DataFrame:
        """
        Aggregate data by group

        Args:
            df (pd.DataFrame): Input DataFrame
            group_by (List[str]): Columns to group by
            agg_columns (List[str]): Columns to aggregate
            agg_func (str): Aggregation function (sum, mean, count, etc.)

        Returns:
            pd.DataFrame: Aggregated data
        """

        valid_group_by = [col for col in group_by if col in df.columns]
        valid_agg = [col for col in agg_columns if col in df.columns]

        if not valid_group_by or not valid_agg:
            return df

        try:
            agg_dict = {col: agg_func for col in valid_agg}
            result = df.groupby(valid_group_by).agg(agg_dict).reset_index()
            return result
        except Exception:
            return df

    def filter_data(self, df: pd.DataFrame, conditions: Dict[str, Any]) -> pd.DataFrame:
        """
        Filter data based on conditions

        Args:
            df (pd.DataFrame): Input DataFrame
            conditions (Dict): Filter conditions {column: value}

        Returns:
            pd.DataFrame: Filtered DataFrame
        """

        for column, value in conditions.items():
            if column in df.columns:
                df = df[df[column] == value]

        return df

    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            Dict: Summary statistics
        """

        return {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
            "missing_values": df.isnull().sum().to_dict(),
            "numeric_summary": df.describe().to_dict(),
        }
