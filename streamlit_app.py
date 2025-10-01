from textwrap import dedent
import streamlit as st

# Author: Raymond Teng Toh Zi

st.set_page_config(page_title="Integrated Market Dashboards", layout="wide", initial_sidebar_state="expanded")

COMMON_CSS = dedent('''
<style>
:root { --accent: #252e28; }
/* General button styles */
.stDownloadButton > button, .stButton > button {
  background: var(--accent) !important;
  color: #ffffff !important;
  border: 0 !important;
  border-radius: 10px !important;
  padding: 0.6rem 1rem !important;
  box-shadow: 0 2px 10px rgba(0,0,0,0.06) !important;
}
.stDownloadButton > button:hover, .stButton > button:hover { filter: brightness(0.95); }

.block-title { font-size:1.2rem; font-weight:700; }
div[data-testid="stMetric"] [data-testid="stMetricLabel"],
div[data-testid="stMetric"] [data-testid="stMetricValue"],
div[data-testid="stMetric"] [data-testid="stMetricValue"] > div {
  white-space: normal !important;
  overflow-wrap: anywhere !important;
  word-break: break-word !important;
  line-height: 1.2 !important;
}
</style>
''')

st.markdown(COMMON_CSS, unsafe_allow_html=True)

from importlib import import_module

DASHBOARDS = {
    "MongoDB (Sentiment & Market)": "mongo_dashboard",
    "Neo4j (Graph Visuals)": "neo4j_dashboard",
    "Spark (Market Data)": "spark_dashboard",
}

with st.sidebar:
    st.header("Integrated Dashboards")
    choice = st.radio("Choose dashboard", list(DASHBOARDS.keys()))
    st.markdown("---")
    st.caption("All dashboards live in separate modules. Select a dashboard to render.")

module_name = DASHBOARDS[choice]
try:
    module = import_module(module_name)
except Exception as e:
    st.error(f"Failed to import module '{module_name}': {e}")
    st.stop()

if hasattr(module, "render"):
    try:
        module.render()
    except Exception as e:
        st.exception(f"Dashboard '{module_name}' raised an exception while rendering:\n{e}")
else:
    st.error(f"Module '{module_name}' does not provide a `render()` function.")