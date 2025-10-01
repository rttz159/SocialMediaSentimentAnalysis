import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import subprocess
import time
import psutil
import plotly.graph_objects as go
from hdfs import InsecureClient

# Author: Eyvon Chieng Chu Sing

HDFS_URL = "http://localhost:9870"
STOP_HDFS_PATH = "/user/student/temp/STOP"
hdfs_client = InsecureClient(HDFS_URL, user="student")

def stop_file_exists():
    return hdfs_client.status(STOP_HDFS_PATH, strict=False) is not None

def create_stop_file():
    try:
        with hdfs_client.write(STOP_HDFS_PATH, encoding="utf-8", overwrite=True) as f:
            f.write("stop")
    except Exception as e:
        st.warning(f"Failed to create HDFS stop file: {e}")

def delete_stop_file():
    try:
        if stop_file_exists():
            hdfs_client.delete(STOP_HDFS_PATH)
    except Exception as e:
        st.warning(f"Failed to remove HDFS stop file: {e}")

def is_process_running(script_name):
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            if script_name in proc.info['cmdline']:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    return False

def kill_process(script_name):
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            if script_name in proc.info['cmdline']:
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

@st.cache_resource
def get_spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("MarketDataDashboard")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

def load_delta_to_pandas(spark, path):
    try:
        df = spark.read.format("delta").load(path)
        if df.rdd.isEmpty():
            return pd.DataFrame(), None
        max_date = df.agg({"date": "max"}).collect()[0][0]
        return df.toPandas(), max_date
    except Exception as e:
        return pd.DataFrame(), None

def run_script_background(script_name, args=[]):
    try:
        cmd = ["python", script_name] + [str(a) for a in args]
        subprocess.Popen(cmd)
    except Exception as e:
        st.error(f"Failed to start {script_name}: {e}")

def create_kafka_topic(topic_name="market_data_topic"):
    try:
        subprocess.run([
            "kafka-topics.sh", "--create",
            "--topic", topic_name,
            "--partitions", "1",
            "--replication-factor", "1",
            "--if-not-exists",
            "--bootstrap-server", "localhost:9092"
        ], check=True)
    except Exception as e:
        st.error(f"Failed to create Kafka topic: {e}")

def start_mocking(days, publish_delay):
    if days <= 0 or publish_delay <= 0:
        st.error("Invalid days or publish delay. Must be positive values.")
        return

    create_kafka_topic()
    run_script_background("mock_producers.py", [days, publish_delay])
    time.sleep(5)
    run_script_background("struct_streaming.py")

if "initialized" not in st.session_state:
    st.session_state.initialized = True

    create_stop_file()
    kill_process("mock_producers.py")
    kill_process("struct_streaming.py")

    hdfs_path = "hdfs://localhost:9000/user/student/task5"
    hdfs_path_2 = "hdfs://localhost:9000/user/student/temp"
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path_2])
    subprocess.run([
        "kafka-topics.sh", "--delete",
        "--topic", "market_data_topic",
        "--bootstrap-server", "localhost:9092"
    ])

    delete_stop_file()
    st.cache_resource.clear() 

def render():
    st.title("Market Data Dashboard (Spark)")

    ohlc_path = "hdfs://localhost:9000/user/student/task5/output/ohlc"
    sector_path = "hdfs://localhost:9000/user/student/task5/output/sector"

    st.sidebar.markdown("### Producer Settings")

    if "days" not in st.session_state:
        st.session_state.days = 180
    days = st.sidebar.number_input(
        "Days to scrape", min_value=30, max_value=180,
        value=st.session_state.days, step=1
    )
    st.session_state.days = days

    if "publish_delay" not in st.session_state:
        st.session_state.publish_delay = 0.5
    publish_delay = st.sidebar.number_input(
        "Publish delay (seconds)", min_value=0.1, max_value=5.0,
        value=st.session_state.publish_delay, step=0.1
    )
    st.session_state.publish_delay = publish_delay

    st.sidebar.markdown("### Mocking Control Panel")

    if "mocking_started" not in st.session_state:
        st.session_state.mocking_started = False
    
    start_mocking_btn = st.sidebar.button(
        "Start Mocking", 
        disabled=st.session_state.mocking_started
    )
    
    if start_mocking_btn:
        st.session_state.mocking_started = True 
        start_mocking(days, publish_delay)

    st.sidebar.markdown("### Status")
    st.sidebar.write(f"**Producer:** {'Running' if is_process_running('mock_producers.py') else 'Not Running'}")
    st.sidebar.write(f"**Consumer:** {'Running' if is_process_running('struct_streaming.py') else 'Not Running'}")

    spark = get_spark()
    ohlc_df, ohlc_max_date = load_delta_to_pandas(spark, ohlc_path)
    sector_df, sector_max_date = load_delta_to_pandas(spark, sector_path)

    if ohlc_df.empty and sector_df.empty:
        st.info("Waiting for market data to appear...")
        st_autorefresh(interval=5_000, key="waiting_for_data")

    fallback_date = pd.Timestamp("2000-01-01")
    latest_date = max([d for d in [ohlc_max_date, sector_max_date] if d is not None], default=fallback_date)
    st_autorefresh(interval=10_000, key=f"refresh_{latest_date}")

    if "days_range" not in st.session_state:
        st.session_state.days_range = 30

    if not ohlc_df.empty:
        ohlc_df['date'] = pd.to_datetime(ohlc_df['date']).dt.date
        ohlc_df = ohlc_df.sort_values(['StockCode', 'date'])
        ohlc_df['prev_close'] = ohlc_df.groupby('StockCode')['close'].shift(1)
        ohlc_df['pct_change'] = ((ohlc_df['close'] - ohlc_df['prev_close']) / ohlc_df['prev_close'] * 100).round(2)

        max_date = ohlc_df['date'].max()
        min_date = ohlc_df['date'].min()
        st.session_state.days_range = st.sidebar.slider(
            "Show last N days:", 1, (max_date - min_date).days + 1, st.session_state.days_range
        )
        filter_start_date = max_date - pd.Timedelta(days=st.session_state.days_range - 1)
        ohlc_df_filtered = ohlc_df[ohlc_df['date'] >= filter_start_date]
    else:
        ohlc_df_filtered = pd.DataFrame()

    if not sector_df.empty:
        sector_df['date'] = pd.to_datetime(sector_df['date']).dt.date
        sector_df = sector_df.sort_values(['Sector', 'date'])
        sector_df_filtered = sector_df[sector_df['date'] >= filter_start_date] if not ohlc_df_filtered.empty else sector_df
    else:
        sector_df_filtered = pd.DataFrame()

    tabs = st.tabs(["Daily % Change", "Sector Volume Heatmap"])

    with tabs[0]:
        if ohlc_df_filtered.empty:
            st.info("No OHLC data to display yet.")
        else:
            stocks = sorted(ohlc_df_filtered['StockCode'].unique())
            selected_stocks = st.multiselect("Select Stocks", stocks, default=stocks)
            df = ohlc_df_filtered[ohlc_df_filtered['StockCode'].isin(selected_stocks)] if selected_stocks else ohlc_df_filtered
            fig = px.line(df, x='date', y='pct_change', color='StockCode', title="Daily % Change per Stock", markers=True)
            fig.update_layout(xaxis_title="Date", yaxis_title="% Change", legend_title="Stock Code")
            st.plotly_chart(fig, use_container_width=True)

    with tabs[1]:
        if sector_df_filtered.empty:
            st.info("No sector volume data to display yet.")
        else:
            sectors = sorted(sector_df_filtered['Sector'].dropna().unique())
            selected_sectors = st.multiselect("Select Sectors", sectors, default=sectors)
            df = sector_df_filtered[sector_df_filtered['Sector'].isin(selected_sectors)] if selected_sectors else sector_df_filtered
            df['avg_volume_7d'] = df.groupby('Sector')['avg_volume'].transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
            df['volume_spike'] = df['avg_volume'] > 1.5 * df['avg_volume_7d']
            heatmap_df = df.pivot(index='Sector', columns='date', values='avg_volume').fillna(0)
            fig = px.imshow(heatmap_df, labels=dict(x="Date", y="Sector", color="Avg Volume"), x=heatmap_df.columns, y=heatmap_df.index, aspect="auto", color_continuous_scale="Viridis")
            fig.update_traces(xgap=1, ygap=1)
            fig.update_layout(title="Sector Avg Volume Heatmap (Spikes Highlighted)")
            st.plotly_chart(fig, use_container_width=True)

