from datetime import datetime
from io import BytesIO
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

# Author: Tan Zi Yang

try:
    from mongo.mongodb_utils import PyMongoUtils
except Exception:
    PyMongoUtils = None

def month_bounds(year: int, month: int):
    start = datetime(year, month, 1)
    end = datetime(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
    return start, end

@st.cache_resource(show_spinner=False)
def get_db():
    if PyMongoUtils is None:
        raise RuntimeError("PyMongoUtils is not implemented. Please implement mongo/mongodb_utils.py")
    return PyMongoUtils().get_database("DE")

@st.cache_data(ttl=300, show_spinner=False)
def monthly_top5(_db, year: int, month: int):
    start, end = month_bounds(year, month)
    pos_field = "$overall.avgPos"
    neg_field = "$overall.avgNeg"
    pipeline = [
        {"$match": {"Date": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": "$StockCode", "avgPos": {"$avg": pos_field}, "avgNeg": {"$avg": neg_field}}},
        {"$addFields": {"net": {"$subtract": ["$avgPos", "$avgNeg"]}}},
        {"$lookup": {"from": "Company", "localField": "_id", "foreignField": "StockCode", "as": "company"}},
        {"$unwind": {"path": "$company", "preserveNullAndEmptyArrays": True}},
        {"$project": {"_id": 0, "StockCode": "$_id", "Company": {"$ifNull": ["$company.Company", "$_id"]},
                      "avgPos": 1, "avgNeg": 1, "net": 1}},
    ]
    return list(_db["Sentiment_Daily"].aggregate(pipeline, allowDiskUse=True))

@st.cache_data(ttl=300, show_spinner=False)
def volume_change_all_companies(_db, start, end):
    pipeline = [
        {"$match": {"Date": {"$gte": start, "$lt": end}}},
        {"$project": {"_id": 0, "StockCode": 1, "Date": 1, "Volume": 1, "Change": 1}},
    ]
    return list(_db["Market_Daily"].aggregate(pipeline, allowDiskUse=True))

@st.cache_data(ttl=300, show_spinner=False)
def get_company_map(_db):
    items = list(_db["Company"].find({}, {"_id": 0, "StockCode": 1, "Company": 1}))
    return {d["StockCode"]: d.get("Company", d["StockCode"]) for d in items}

def fig_to_png_bytes(fig) -> bytes:
    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=180, bbox_inches="tight")
    buf.seek(0)
    return buf.getvalue()

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def plot_diverging_bars(corrs: pd.DataFrame, title="Volume–Price correlation", invert_y=True):
    df = corrs.copy()
    fig, ax = plt.subplots(figsize=(12, max(6, 0.4 * len(df))))
    colors = plt.cm.coolwarm((df["Corr"] + 1) / 2)
    ax.barh(df["Company"], df["Corr"], color=colors)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_xlabel("Corr")
    ax.set_title(title)
    if invert_y:
        ax.invert_yaxis()
    fig.tight_layout()
    return fig

def render():
    st.title("Financial Market & Sentiment Dashboard (MongoDB)")

    with st.sidebar:
        st.markdown("### Options")
        year = st.selectbox("Year", [2024, 2025], index=1)
        month = st.selectbox("Month", list(range(1, 13)), index=6)

    def header_block(title: str):
        st.subheader(title)
        st.markdown("---")

    try:
        db = get_db()
    except Exception as e:
        st.warning(f"MongoDB not available: {e}")
        st.info("Implement or configure mongo/mongodb_utils.py and ensure MongoDB is reachable.")
        return

    tab1, tab2 = st.tabs(["Top 5 Sentiment Company", "Price Change vs Volume Correlation"])

    with tab1:
        header_block(f"Top 5 Sentiment Companies — {year}-{month:02d}")
        
        with st.expander("Sentiment Options"):
            sentiment = st.selectbox("Select sentiment metric", ["Positive", "Negative", "Net"], key="sb_sentiment")

        data = monthly_top5(db, year, month)
        if not data:
            st.warning("No data for this month.")
            return

        df = pd.DataFrame(data)
        col_map = {"Positive": "avgPos", "Negative": "avgNeg", "Net": "net"}
        value_col = col_map[sentiment]
        df_sorted = df.sort_values(value_col, ascending=False).head(5).reset_index(drop=True)

        left, right = st.columns([7, 3])
        with left:
            fig, ax = plt.subplots(figsize=(10, 5))
            vals = df_sorted[value_col].to_numpy()
            vmin, vmax = float(vals.min()), float(vals.max())
            if abs(vmax - vmin) < 1e-9:
                vmin -= 1e-6
                vmax += 1e-6
            cmap = plt.cm.Blues if sentiment == "Positive" else (plt.cm.Reds if sentiment == "Negative" else plt.cm.Greens)
            norm = plt.Normalize(vmin=vmin, vmax=vmax)
            colors = cmap(norm(vals))
            ax.bar(df_sorted["Company"], vals, color=colors, edgecolor="#1f2937", linewidth=1.0)
            ax.set_title(f"Top 5 {sentiment} Sentiment Companies ({year}-{month:02d})")
            ax.set_ylabel(sentiment)
            ax.set_xlabel("Company")
            plt.xticks(rotation=20, ha="right")
            st.pyplot(fig, use_container_width=True)
            png_bytes = fig_to_png_bytes(fig)

        with right:
            top_row = df_sorted.iloc[0]
            st.metric(label="Top 1 Company", value=str(top_row["Company"]), delta=f"{sentiment}: {top_row[value_col]:.3f}")
            st.subheader("Data Table")
            st.dataframe(df_sorted, hide_index=True, use_container_width=True)
            csv_bytes = df_to_csv_bytes(df_sorted)
            c1, c2 = st.columns([1, 1], gap="small")
            with c1:
                st.download_button("Download chart PNG", data=png_bytes, file_name=f"top5_{sentiment}_{year}{month:02d}.png", mime="image/png", use_container_width=True)
            with c2:
                st.download_button("Download data CSV", data=csv_bytes, file_name=f"top5_{sentiment}_{year}{month:02d}.csv", mime="text/csv", use_container_width=True)

    with tab2:
        header_block(f"Volume-Price Correlation — {year}-{month:02d}")
        
        with st.expander("Sort Options"):
            sort_by = st.selectbox("Sort by", ["Correlation Desc", "Correlation Asc", "A -> Z"], key="sb_sort")

        start, end = month_bounds(year, month)
        rows = volume_change_all_companies(db, start, end)
        if not rows:
            st.warning("No data for this month.")
            return

        df = pd.DataFrame(rows)
        if df.empty:
            st.warning("No data for this month.")
            return

        df["Date"] = pd.to_datetime(df["Date"]) if "Date" in df.columns else df
        corrs = (
            df.groupby("StockCode")
            .apply(lambda g: g["Volume"].corr(g["Change"]))
            .dropna()
            .reset_index(name="Corr")
        )

        if corrs.empty:
            st.info("Not enough data to compute correlations.")
            return

        company_map = get_company_map(db)
        corrs["Company"] = corrs["StockCode"].map(company_map).fillna(corrs["StockCode"])

        if sort_by == "Correlation Asc":
            corrs_disp = corrs.sort_values("Corr", ascending=True)
        elif sort_by == "A -> Z":
            corrs_disp = corrs.sort_values("Company", ascending=True)
        else:
            corrs_disp = corrs.sort_values("Corr", ascending=False)

        left, right = st.columns([7, 3])
        with left:
            fig = plot_diverging_bars(corrs_disp, title=f"Volume–Price correlation ({year}-{month:02d})")
            st.pyplot(fig, use_container_width=True)
            png_bytes = fig_to_png_bytes(fig)

        with right:
            top_pos = corrs.loc[corrs["Corr"].idxmax()]
            top_neg = corrs.loc[corrs["Corr"].idxmin()]
            st.metric("Highest positive correlation", f"{top_pos['Company']}", f"{top_pos['Corr']:.2f}")
            st.metric("Highest negative correlation", f"{top_neg['Company']}", f"{top_neg['Corr']:.2f}")
            st.subheader("Correlation ranking")
            corrs_ranked = corrs_disp.reset_index(drop=True)
            corrs_ranked.index = corrs_ranked.index + 1
            corrs_ranked.index.name = "Rank"
            st.dataframe(corrs_ranked[["Company", "StockCode", "Corr"]], use_container_width=True)
            csv_bytes = df_to_csv_bytes(corrs_ranked.reset_index())
            c1, c2 = st.columns([1, 1], gap="small")
            with c1:
                st.download_button("Download bars PNG", data=png_bytes, file_name=f"corr_bars_{year}{month:02d}.png", mime="image/png")
            with c2:
                st.download_button("Download ranking CSV", data=csv_bytes, file_name=f"corr_rank_{year}{month:02d}.csv", mime="text/csv")
