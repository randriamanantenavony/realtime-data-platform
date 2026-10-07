from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
from deltalake import DeltaTable


# ============================================================
# PROJECT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

SUMMARY_PATH = GOLD_PATH / "sentiment_summary"
LANGUAGE_PATH = GOLD_PATH / "sentiment_by_language"
TIME_PATH = GOLD_PATH / "sentiment_over_time"
TECHNOLOGY_PATH = GOLD_PATH / "technology_trends"


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Tech Sentiment Analytics",
    page_icon="📊",
    layout="wide"
)


# ============================================================
# DELTA READER
# ============================================================

def read_delta(path: Path) -> pd.DataFrame:
    """
    Read the latest snapshot of a Delta table.
    """

    delta_table = DeltaTable(str(path))

    return delta_table.to_pandas()


# ============================================================
# DASHBOARD
# ============================================================

@st.fragment(run_every="10s")
def dashboard():

    try:

        # ====================================================
        # LOAD GOLD TABLES
        # ====================================================

        sentiment_summary = read_delta(
            SUMMARY_PATH
        )

        sentiment_by_language = read_delta(
            LANGUAGE_PATH
        )

        sentiment_over_time = read_delta(
            TIME_PATH
        )

        technology_trends = read_delta(
            TECHNOLOGY_PATH
        )


        # ====================================================
        # TITLE
        # ====================================================

        st.title(
            "📊 Tech Sentiment Analytics"
        )

        st.caption(
            "Real-time social media sentiment monitoring "
            "with Kafka, Spark, Delta Lake and NLP"
        )


        # ====================================================
        # KPIs
        # ====================================================

        total_posts = int(
            sentiment_summary["post_count"].sum()
        )


        def get_sentiment_count(sentiment):

            result = sentiment_summary[
                sentiment_summary["sentiment"]
                == sentiment
            ]

            if result.empty:
                return 0

            return int(
                result["post_count"].iloc[0]
            )


        positive = get_sentiment_count(
            "positive"
        )

        neutral = get_sentiment_count(
            "neutral"
        )

        negative = get_sentiment_count(
            "negative"
        )


        col1, col2, col3, col4 = st.columns(4)


        col1.metric(
            "Total posts",
            f"{total_posts:,}"
        )

        col2.metric(
            "🟢 Positive",
            positive,
            f"{positive / total_posts * 100:.1f}%"
            if total_posts
            else "0%"
        )

        col3.metric(
            "⚪ Neutral",
            neutral,
            f"{neutral / total_posts * 100:.1f}%"
            if total_posts
            else "0%"
        )

        col4.metric(
            "🔴 Negative",
            negative,
            f"{negative / total_posts * 100:.1f}%"
            if total_posts
            else "0%"
        )


        st.divider()


        # ====================================================
        # SENTIMENT DISTRIBUTION
        # ====================================================

        st.subheader(
            "Global sentiment distribution"
        )

        sentiment_chart = (
            sentiment_summary
            .set_index("sentiment")[
                "post_count"
            ]
        )

        st.bar_chart(
            sentiment_chart
        )


        st.divider()


        # ====================================================
        # SENTIMENT OVER TIME
        # ====================================================

        st.subheader("📈 Sentiment over time")

        if not sentiment_over_time.empty:

            sentiment_over_time["event_date"] = pd.to_datetime(
                sentiment_over_time["event_date"]
            )

            time_chart = (
                sentiment_over_time
                .pivot_table(
                    index="event_date",
                    columns="sentiment",
                    values="post_count",
                    aggfunc="sum",
                    fill_value=0
                )
                .sort_index()
            )

            st.line_chart(
                time_chart,
                width="stretch"
            )

        else:
            st.info("No temporal data available.")


        # ====================================================
        # TWO-COLUMN SECTION
        # ====================================================

        left, right = st.columns(2)


        # ====================================================
        # LANGUAGE
        # ====================================================

        with left:

            st.subheader(
                "🌍 Sentiment by language"
            )


            language_chart = (
                sentiment_by_language

                .pivot_table(
                    index="language",
                    columns="sentiment",
                    values="post_count",
                    aggfunc="sum",
                    fill_value=0
                )
            )


            language_chart[
                "total"
            ] = language_chart.sum(
                axis=1
            )


            language_chart = (
                language_chart

                .sort_values(
                    "total",
                    ascending=False
                )

                .head(10)

                .drop(
                    columns="total"
                )
            )


            st.bar_chart(
                language_chart
            )


        # ====================================================
        # TECHNOLOGY
        # ====================================================

        with right:

            st.subheader(
                "💻 Technology trends"
            )


            technology_chart = (
                technology_trends

                .pivot_table(
                    index="technology",
                    columns="sentiment",
                    values="post_count",
                    aggfunc="sum",
                    fill_value=0
                )
            )


            technology_chart[
                "total"
            ] = technology_chart.sum(
                axis=1
            )


            technology_chart = (
                technology_chart

                .sort_values(
                    "total",
                    ascending=False
                )

                .head(10)

                .drop(
                    columns="total"
                )
            )


            st.bar_chart(
                technology_chart
            )


        # ====================================================
        # RAW GOLD DATA
        # ====================================================

        with st.expander(
            "🔎 View Gold data"
        ):

            tab1, tab2, tab3, tab4 = st.tabs([
                "Summary",
                "Languages",
                "Timeline",
                "Technologies"
            ])


            with tab1:

                st.dataframe(
                    sentiment_summary,
                    width="stretch",
                    hide_index=True
                )


            with tab2:

                st.dataframe(
                    sentiment_by_language,
                    width="stretch",
                    hide_index=True
                )


            with tab3:

                st.dataframe(
                    sentiment_over_time,
                    width="stretch",
                    hide_index=True
                )


            with tab4:

                st.dataframe(
                    technology_trends,
                    width="stretch",
                    hide_index=True
                )


        # ====================================================
        # STATUS
        # ====================================================

        st.divider()

        st.caption(
            "Last dashboard refresh: "
            + datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        st.caption(
            "Dashboard refresh interval: 10 seconds"
        )


    except Exception as error:

        st.error(
            f"Unable to load Gold Delta tables: {error}"
        )


# ============================================================
# RUN
# ============================================================

dashboard()