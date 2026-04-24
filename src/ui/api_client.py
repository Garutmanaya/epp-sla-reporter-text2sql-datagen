import requests
import streamlit as st

API_URL = "http://localhost:8080/predict"

def get_sql_prediction(question: str):
    """Sends user query to FastAPI and returns the generated SQL."""
    payload = {"question": question, "db_id": "epp_registry"}
    try:
        response = requests.post(API_URL, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Backend API Error: {e}")
        return None
