from typing import Optional
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

# Author: Lewis Lim Rong Zhen

class T4Visualisation:
    @staticmethod
    def sentiment_pies(df: pd.DataFrame, company_name: str,
                       start_date: str, end_date: str,
                       source: Optional[str] = None):
        if df.empty:
            return None
        df_total = df.groupby("sentiment_class", as_index=False)["count"].sum()
        df_total["type"] = "Total"
        df_all = pd.concat([df, df_total], ignore_index=True)
        sentiment_order = ["Negative", "Neutral", "Positive"]
        fig, axes = plt.subplots(1, 3, figsize=(16, 6))
        for ax, dtype in zip(axes, ["Post", "Comment", "Total"]):
            subset = (
                df_all[df_all["type"] == dtype]
                .set_index("sentiment_class")
                .reindex(sentiment_order)
                .fillna(0)
                .reset_index()
            )
            if subset["count"].sum() == 0:
                ax.set_title(f"{dtype} Sentiment (no data)")
                continue
            sizes = subset["count"].tolist()
            total = sum(sizes)
            labels = [
                f"{cls} ({int(cnt)} | {cnt*100/total:.1f}%)"
                for cls, cnt in zip(subset["sentiment_class"], subset["count"])
            ]
            wedges, _ = ax.pie(
                sizes,
                startangle=140,
                colors=["red", "gray", "green"],
            )
            ax.set_title(f"{dtype} Sentiment")
            ax.legend(
                wedges,
                labels,
                title="Sentiment",
                loc="center left",
                bbox_to_anchor=(1, 0, 0.5, 1),
            )
        fig.suptitle(
            f"Sentiment Distribution for {company_name}\n"
            f"{start_date} → {end_date} | Source={source or 'All'}"
        )
        fig.tight_layout()
        return fig

    @staticmethod
    def sentiment_table(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["sentiment_class", "Post", "Comment", "Total"])
        pivot = df.pivot(index="sentiment_class", columns="type", values="count") \
                  .fillna(0).astype(int)
        for s in ["Negative", "Neutral", "Positive"]:
            if s not in pivot.index:
                pivot.loc[s] = [0] * len(pivot.columns)
        pivot["Total"] = pivot.sum(axis=1)
        pivot = pivot.loc[["Negative", "Neutral", "Positive"]]
        return pivot.reset_index()

    @staticmethod
    def stock_vs_social_chart(df: pd.DataFrame, company_name: str):
        if df.empty:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"].astype(str), errors="coerce")
        for col in ["close", "volume", "post_count", "comment_count", "social_activity"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df = df.sort_values("date")
        df = df.resample("W", on="date").agg({
            "close": "mean",
            "post_count": "sum",
            "comment_count": "sum",
            "social_activity": "sum"
        }).reset_index()
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax1.bar(df["date"], df["post_count"], color="lightblue", label="Posts")
        ax1.bar(df["date"], df["comment_count"], bottom=df["post_count"],
                color="steelblue", label="Comments")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Social Activity Count")
        for x, total in zip(df["date"], df["social_activity"]):
            ax1.text(x, total, str(int(total)), ha="center", va="bottom", fontsize=8)
        ymax = (df["social_activity"].max() or 0) * 1.15 + 1
        ax1.set_ylim(0, ymax)
        ax2 = ax1.twinx()
        ax2.plot(df["date"], df["close"], color="red", marker="o",
                 linewidth=2, label="Close Price")
        ax2.set_ylabel("Close Price")
        ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        plt.setp(ax1.get_xticklabels(), rotation=45, ha="right")
        fig.suptitle(f"{company_name}: Close Price vs Posts & Comments (Weekly Aggregated)")
        fig.tight_layout()
        return fig
