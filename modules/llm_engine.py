import streamlit as st
import json
import re
from google import genai


class LLMEngine:
    def __init__(self):
        api_key = st.secrets["API_KEY"]
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")

        self.client = genai.Client(api_key=api_key)
        self.model_name = "models/gemini-flash-lite-latest"

    def generate_sql(self, prompt: str) -> str:
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt
        )

        raw_text = response.text
        return self._extract_json(raw_text)

    def _extract_json(self, text: str) -> str:
        """
        Extract first valid JSON object from LLM output
        """
        # Remove markdown blocks
        text = re.sub(r"```(?:json)?", "", text)
        text = text.strip()

        # Find first JSON object
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError("No JSON object found in LLM response")

        json_text = match.group(0)

        # Validate JSON
        json.loads(json_text)
        return json_text
