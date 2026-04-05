"""
Prompt Engineering Module
Creates optimized prompts for LLM SQL generation
"""

import json
from typing import Dict, Any


class PromptTemplate:
    """
    Generates schema-aware prompts for SQL generation
    Ensures LLM produces valid, executable SQL
    """

    def __init__(self, schema: str):
        """
        Initialize prompt template

        Args:
            schema (str): Database schema CREATE TABLE statement
        """
        self.schema = schema

    def create_prompt(self, natural_query: str) -> str:
        """
        Create optimized prompt for LLM

        Args:
            natural_query (str): User's natural language query

        Returns:
            str: Formatted prompt for LLM
        """

        prompt = f"""
You are an expert SQL analyst. Your task is to convert natural language queries into valid SQL.

DATABASE SCHEMA:
{self.schema}

IMPORTANT RULES:
1. Only use columns that exist in the schema above
2. Use appropriate SQL aggregations (SUM, COUNT, AVG, MAX, MIN)
3. Use GROUP BY when aggregating
4. Use ORDER BY for sorting results
5. Only SELECT queries - NO INSERT, UPDATE, DELETE
6. Handle NULL values appropriately
7. Return results in a meaningful order
8. IMPORTANT DATA MATCHING RULES:
   - Categorical values (like crop names, country names) may have variations
   - Use case-insensitive matching
   - Prefer LIKE with wildcards (%) instead of exact equality when filtering text
   - Example: WHERE Item LIKE '%Rice%' instead of Item = 'Rice'


USER QUERY:
{natural_query}

Generate a response in this exact JSON format.
CRITICAL:
- Return ONLY valid JSON
- Do NOT include explanations
- Do NOT include markdown or code blocks
{{
    "sql_query": "SELECT ... FROM data WHERE ...",
    "chart_type": "bar|line|pie|scatter|area|box|table",
    "axis_mapping": {{
        "x_axis": "column_name",
        "y_axis": "column_name",
        "color_axis": "column_name_or_null"
    }},
    "description": "Brief description of what the query returns"
}}

RESPONSE:
"""
        return prompt

    def create_advanced_prompt(
        self,
        natural_query: str,
        selected_columns: list = None,
        aggregations: Dict[str, str] = None,
    ) -> str:
        """
        Create advanced prompt with hints

        Args:
            natural_query (str): User's query
            selected_columns (list): Suggested columns
            aggregations (Dict): Suggested aggregations

        Returns:
            str: Enhanced prompt
        """

        hint_text = ""

        if selected_columns:
            hint_text += f"\nSuggested columns to use: {', '.join(selected_columns)}"

        if aggregations:
            hint_text += f"\nSuggested aggregations: {json.dumps(aggregations)}"

        prompt = f"""
You are an expert SQL analyst. Your task is to convert natural language queries into valid SQL.

DATABASE SCHEMA:
{self.schema}

OPTIMIZATION HINTS:
{hint_text}

USER QUERY:
{natural_query}

Generate a response in this exact JSON format:
{{
    "sql_query": "SELECT ... FROM data",
    "chart_type": "bar|line|pie|scatter|area|box|table",
    "axis_mapping": {{"x_axis": "column", "y_axis": "column", "color_axis": null}},
    "description": "Description"
}}
"""
        return prompt

    @staticmethod
    def validate_json_response(response: str) -> Dict[str, Any]:
        """
        Validate and parse LLM response

        Args:
            response (str): LLM response text

        Returns:
            Dict: Parsed JSON

        Raises:
            ValueError: If response is invalid
        """

        # Try to extract JSON from response
        try:
            # If response contains markdown code blocks, extract JSON
            if "```json" in response:
                json_str = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_str = response.split("```")[1].split("```")[0].strip()
            else:
                json_str = response

            # Parse JSON
            data = json.loads(json_str)

            # Validate required fields
            required_fields = ["sql_query", "chart_type", "axis_mapping", "description"]
            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")

            return data

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {str(e)}")

    @staticmethod
    def create_safety_prompt(query: str) -> str:
        """
        Create safety validation prompt

        Args:
            query (str): SQL query to validate

        Returns:
            str: Validation prompt
        """

        return f"""
Analyze this SQL query for safety issues:

{query}

Check if:
1. Query contains only SELECT operations
2. No DROP, DELETE, INSERT, UPDATE commands
3. No stored procedure calls
4. No privilege escalation attempts

Respond with JSON:
{{"safe": true/false, "reason": "explanation"}}
"""
