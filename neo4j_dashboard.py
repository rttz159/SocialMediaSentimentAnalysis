import os, sys, datetime as dt
import streamlit as st

# Author: LEWIS LIM RONG ZHEN

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLASSES_DIR = os.path.join(BASE_DIR, "T4_Classes")
if CLASSES_DIR not in sys.path:
    sys.path.append(CLASSES_DIR)

from graph.t4config import Config
from graph.t4queries import T4Queries
from graph.t4visualisation import T4Visualisation

def render():
    st.title("Neo4j Dashboard")

    try:
        driver = Config.get_driver()
        q = T4Queries(driver)
        companies = q.list_companies()
    except Exception as e:
        st.error(f"Failed to connect to Neo4j: {e}")
        companies = []

    if not companies:
        st.info("No companies found in Neo4j. Please run data load first (via task4.py) or check connection.")
        return

    with st.sidebar:
        st.subheader("Filters")
        company_name = st.selectbox("Company", options=companies, index=0)
        source = st.selectbox("Source", options=["All", "Reddit", "Lowyat"], index=0)
        source_val = None if source == "All" else source
        start_date = st.date_input("Start date", dt.date(2014,1,1))
        end_date = st.date_input("End date", dt.date(2025,12,31))

    def header_block(title: str):
        st.subheader(title)
        st.markdown("---")

    q1_tab, q2_tab = st.tabs([
        "Query 1: Sentiment Distribution",
        "Query 2: Stock Close vs Social Activity"
    ])

    with q1_tab:
        header_block("Query 1: Sentiment Distribution (Posts vs Comments)")
        try:
            df = q.sentiment_distribution(
                company_name=company_name,
                start_date=str(start_date),
                end_date=str(end_date),
                source=source_val
            )
            if df.empty:
                st.warning("No matching records for the selected filters.")
            else:
                fig = T4Visualisation.sentiment_pies(
                    df, company_name, str(start_date), str(end_date), source_val
                )
                if fig is not None:
                    st.pyplot(fig, clear_figure=True, use_container_width=True)
                table = T4Visualisation.sentiment_table(df)
                st.dataframe(table, use_container_width=True)
        except Exception as e:
            st.error(f"Query failed: {e}")

    with q2_tab:
        header_block("Query 2: Stock Close vs Social Activity (Daily)")
        try:
            df = q.stock_vs_social(
                company_name=company_name,
                start_date=str(start_date),
                end_date=str(end_date),
                source=source_val
            )
            if df.empty:
                st.warning("No matching records for the selected filters.")
            else:
                # Graph on top
                fig = T4Visualisation.stock_vs_social_chart(df, company_name)
                if fig is not None:
                    st.pyplot(fig, clear_figure=True, use_container_width=True)
                # Table below
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Query failed: {e}")

    try:
        driver.close()
    except Exception:
        pass
