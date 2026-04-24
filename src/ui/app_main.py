import streamlit as st
import pandas as pd
import plotly.express as px
from ui.utils_s3 import download_db_from_s3
from ui.api_client import get_sql_prediction
from ui.db_executor import run_query

# --- CONFIGURATION & THEME ---
st.set_page_config(page_title="EPP SLA Analytics", layout="wide", page_icon="📊")

# Custom CSS for "Attractive UI"
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stTextInput enumerate { color: #2e4053; }
    .query-history { font-size: 0.8rem; color: #7f8c8d; }
    </style>
    """, unsafe_base_value=True)

# --- SESSION STATE ---
if "history" not in st.session_state:
    st.session_state.history = []

# --- APP STARTUP ---
@st.cache_resource
def startup_sync():
    download_db_from_s3()

startup_sync()

# --- SIDEBAR: Theme & History ---
with st.sidebar:
    st.title("Settings")
    theme = st.selectbox("UI Theme", ["Light", "Dark", "Corporate Blue"])
    
    st.divider()
    st.subheader("Last 10 Queries")
    for h in st.session_state.history[-10:]:
        st.caption(f"🕒 {h}")

# --- MAIN UI ---
st.title("📊 EPP SLA Reporter")
st.markdown("### Natural Language to Business Intelligence")

# Example Queries
st.info("**Try these:**\n"
        "* Show average latency for AtlasRegistrar in EU yesterday\n"
        "* List top 5 deployment windows for v2 last month\n"
        "* Show total volume of requests grouped by region")

# User Input
user_query = st.chat_input("Ask a question about SLA data...")

if user_query:
    # 1. Update History
    st.session_state.history.append(user_query)
    
    # 2. Get Prediction
    with st.spinner("Translating to SQL..."):
        prediction = get_sql_prediction(user_query)
    
    if prediction:
        generated_sql = prediction.get("sql")
        
        # UI Layout: Two Columns
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("Generated SQL")
            st.code(generated_sql, language="sql")
            
            # 3. Execute Query
            with st.spinner("Executing query on epp_registry.db..."):
                df = run_query(generated_sql)
            
            if isinstance(df, pd.DataFrame):
                st.subheader("Data Table")
                st.dataframe(df, use_container_width=True)
            else:
                st.error(df) # Shows SQL Error string

        with col2:
            st.subheader("Data Visualization")
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Basic Auto-Graph Logic
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0 and len(df.columns) > 1:
                    x_axis = df.columns[0]
                    y_axis = numeric_cols[0]
                    
                    fig = px.bar(df, x=x_axis, y=y_axis, 
                                 title=f"{y_axis} by {x_axis}",
                                 template="plotly_white",
                                 color_discrete_sequence=['#3498db'])
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.write("Insufficient numeric data for graphing.")
            else:
                st.write("No data available to visualize.")

# Footer
st.divider()
st.caption("Powered by Google Flan-T5 LoRA | EPP Registry Analytics Engine")
