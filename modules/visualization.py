"""
Visualization Module
Handles chart selection and rendering using Plotly
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, Any, Optional


class VisualizationEngine:
    """
    Manages automatic chart selection and rendering
    Supports multiple chart types with intelligent defaults
    """

    def __init__(self):
        """Initialize visualization engine"""
        self.chart_types = [
            "bar",
            "line",
            "pie",
            "scatter",
            "area",
            "box",
            "table",
        ]

    def render_chart(
        self, df: pd.DataFrame, chart_type: str, axis_mapping: Dict[str, str]
    ) -> go.Figure:
        """
        Render chart based on type and data

        Args:
            df (pd.DataFrame): Data to visualize
            chart_type (str): Type of chart
            axis_mapping (Dict): Mapping of axes to columns

        Returns:
            go.Figure: Plotly figure object
        """

        chart_type = chart_type.lower().strip()

        # Get axis columns
        x_axis = axis_mapping.get("x_axis", df.columns[0] if len(df.columns) > 0 else None)
        y_axis = axis_mapping.get("y_axis", df.columns[1] if len(df.columns) > 1 else None)
        color_axis = axis_mapping.get("color_axis", None)

        # Validate columns exist
        if x_axis and x_axis not in df.columns:
            x_axis = df.columns[0]
        if y_axis and y_axis not in df.columns:
            y_axis = df.columns[1] if len(df.columns) > 1 else df.columns[0]

        # Route to appropriate chart renderer
        if chart_type == "bar":
            return self._render_bar(df, x_axis, y_axis, color_axis)
        elif chart_type == "line":
            return self._render_line(df, x_axis, y_axis, color_axis)
        elif chart_type == "pie":
            return self._render_pie(df, x_axis, y_axis)
        elif chart_type == "scatter":
            return self._render_scatter(df, x_axis, y_axis, color_axis)
        elif chart_type == "area":
            return self._render_area(df, x_axis, y_axis, color_axis)
        elif chart_type == "box":
            return self._render_box(df, y_axis, color_axis)
        elif chart_type == "table":
            return self._render_table(df)
        else:
            # Default to table
            return self._render_table(df)

    def _render_bar(
    self, df: pd.DataFrame, x_axis: str, y_axis: str, color_axis: Optional[str] = None
) -> go.Figure:
        """Render bar chart"""

        if color_axis and color_axis in df.columns:
            fig = px.bar(df, x=x_axis, y=y_axis, color=color_axis, title="Bar Chart")
        else:
            fig = px.bar(df, x=x_axis, y=y_axis, title="Bar Chart")

        fig.update_layout(
            hovermode="x unified",
            template="plotly_white",
            height=500,
            showlegend=True if color_axis and color_axis in df.columns else False,
        )
        return fig

    def _render_line(
    self, df: pd.DataFrame, x_axis: str, y_axis: str, color_axis: Optional[str] = None
) -> go.Figure:
        """Render line chart (robust)"""

        # Ensure Y is numeric
        df = df.copy()
        df[y_axis] = pd.to_numeric(df[y_axis], errors="coerce")

        # Sort X-axis if possible
        if pd.api.types.is_datetime64_any_dtype(df[x_axis]):
            df = df.sort_values(by=x_axis)
        elif pd.api.types.is_numeric_dtype(df[x_axis]):
            df = df.sort_values(by=x_axis)
        else:
            # ❌ Categorical X → Line chart not suitable
            # ✔ Fallback to bar chart
            return self._render_bar(df, x_axis, y_axis, color_axis)

        # Render line chart
        if color_axis and color_axis in df.columns:
            fig = px.line(df, x=x_axis, y=y_axis, color=color_axis, title="Line Chart")
        else:
            fig = px.line(df, x=x_axis, y=y_axis, title="Line Chart")

        fig.update_layout(
            hovermode="x unified",
            template="plotly_white",
            height=500,
            showlegend=True if color_axis and color_axis in df.columns else False,
        )

        return fig


    def _render_pie(self, df: pd.DataFrame, x_axis: str, y_axis: str) -> go.Figure:
        """Render pie chart"""

        fig = px.pie(
            df, names=x_axis, values=y_axis, title="Pie Chart", hole=0
        )
        fig.update_layout(
            template="plotly_white",
            height=500,
        )
        return fig

    def _render_scatter(
        self, df: pd.DataFrame, x_axis: str, y_axis: str, color_axis: Optional[str] = None
    ) -> go.Figure:
        """Render scatter chart"""

        if color_axis and color_axis in df.columns:
            fig = px.scatter(df, x=x_axis, y=y_axis, color=color_axis, title="Scatter Plot")
        else:
            fig = px.scatter(df, x=x_axis, y=y_axis, title="Scatter Plot")

        fig.update_layout(
            hovermode="closest",
            template="plotly_white",
            height=500,
            showlegend=True if color_axis and color_axis in df.columns else False,
        )
        return fig

    def _render_area(
        self, df: pd.DataFrame, x_axis: str, y_axis: str, color_axis: Optional[str] = None
    ) -> go.Figure:
        """Render area chart"""

        if color_axis and color_axis in df.columns:
            fig = px.area(df, x=x_axis, y=y_axis, color=color_axis, title="Area Chart")
        else:
            fig = px.area(df, x=x_axis, y=y_axis, title="Area Chart")

        fig.update_layout(
            hovermode="x unified",
            template="plotly_white",
            height=500,
            showlegend=True if color_axis and color_axis in df.columns else False,
        )
        return fig

    def _render_box(self, df: pd.DataFrame, y_axis: str, color_axis: Optional[str] = None) -> go.Figure:
        """Render box plot"""

        if color_axis and color_axis in df.columns:
            fig = px.box(df, y=y_axis, color=color_axis, title="Box Plot")
        else:
            fig = px.box(df, y=y_axis, title="Box Plot")

        fig.update_layout(
            template="plotly_white",
            height=500,
            showlegend=True if color_axis and color_axis in df.columns else False,
        )
        return fig

    def _render_table(self, df: pd.DataFrame) -> go.Figure:
        """Render data table"""

        fig = go.Figure(
            data=[
                go.Table(
                    header=dict(
                        values=list(df.columns),
                        fill_color="paleturquoise",
                        align="left",
                        font=dict(color="black", size=12),
                    ),
                    cells=dict(
                        values=[df[col] for col in df.columns],
                        fill_color="lavender",
                        align="left",
                        font=dict(color="black", size=11),
                        height=25,
                    ),
                )
            ]
        )
        fig.update_layout(height=600, title="Data Table")
        return fig

    def auto_select_chart_type(self, df: pd.DataFrame, query: str) -> str:
        """
        Automatically select best chart type based on data shape

        Args:
            df (pd.DataFrame): Input data
            query (str): Natural language query

        Returns:
            str: Recommended chart type
        """

        rows, cols = df.shape

        # Query-based heuristics
        query_lower = query.lower()

        if "distribution" in query_lower or "frequency" in query_lower:
            return "bar"
        elif "trend" in query_lower or "over time" in query_lower:
            return "line"
        elif "breakdown" in query_lower or "composition" in query_lower:
            return "pie"
        elif "comparison" in query_lower or "versus" in query_lower:
            return "scatter"

        # Data shape based heuristics
        if cols == 1:
            return "table"
        elif rows > 100 and cols == 2:
            return "scatter"
        elif rows <= 10:
            return "bar"
        elif rows > 50:
            return "line"
        else:
            return "bar"

    def validate_chart_data(self, df: pd.DataFrame, chart_type: str) -> bool:
        """
        Validate if data is suitable for chart type

        Args:
            df (pd.DataFrame): Input data
            chart_type (str): Chart type

        Returns:
            bool: Whether data is valid
        """

        if df.empty:
            return False

        if chart_type in ["bar", "line", "scatter"]:
            return len(df.columns) >= 2
        elif chart_type in ["pie"]:
            return len(df.columns) >= 2
        elif chart_type in ["table"]:
            return True

        return True
