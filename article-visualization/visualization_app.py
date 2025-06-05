import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import datetime

# Streamlit config
st.set_page_config(page_title="News Visualizer", layout="wide")
st.title("Maryland News Articles Visualization Dashboard")
st.markdown("This dashboard visualizes articles from the Capital Gazette, Hyattsville Wire, and Baltimore Banner. Use the filters to explore different sources and date ranges. Data was collected starting on April 28th, 2025, however, the Hyattsville Wire had articles dated well prior to then that we were able to still collect.")
st.markdown("""
**Developed by the Data Visualization team at the Digital Engagement Lab**  
**Directed by:** Noah Der Garabedian  
**Contributors:** Justin Lee, Sivani Dronamraju, Sean Gunshenan  
""")
# Load data from PostgreSQL
@st.cache_data(ttl=600)
def load_data():
    engine = create_engine(
        "postgresql+psycopg2://scraperdb_owner:npg_mbyWDf3q5rFp@ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/scraperdb?sslmode=require"
    )
    cg = pd.read_sql_query("SELECT * FROM capitol_gazette;", engine)
    hw = pd.read_sql_query("SELECT * FROM hyattsville_wire;", engine)
    bb = pd.read_sql_query("SELECT * FROM baltimore_banner;", engine)
    cg["source"] = "Capital Gazette"
    hw["source"] = "Hyattsville Wire"
    cg = cg[cg['word_count'] > 30]  # remove paywalled articles from CG
    bb["source"] = "Baltimore Banner"
    # Remove timezone from pub_date if present
    if hasattr(hw["pub_date"], 'dt'):
        hw["pub_date"] = hw["pub_date"].dt.tz_localize(None)
    if hasattr(bb["pub_date"], 'dt'):
        bb["pub_date"] = bb["pub_date"].dt.tz_localize(None)
    df = pd.concat([cg, hw, bb], ignore_index=True)
    df = df.dropna(subset=["headline", "pub_date"])
    df["pub_date"] = pd.to_datetime(df["pub_date"], errors="coerce")
    df = df.dropna(subset=["pub_date"])
    df = df[(df["headline_len"] > 0) & (df["word_count"] > 0)]
    return df

df = load_data()



#remove outliers
df = df[df["word_count"] < 5000]  # Remove articles with excessive word count
#print(f"HW articles after filtering: {len(df[df['source'] == 'Hyattsville Wire'])}")

# Sidebar filters
st.sidebar.header("🔎 Filters")

sources = st.sidebar.multiselect(
    "Select News Sources",
    options=df["source"].unique(),
    default=df["source"].unique()
)

# Set default dates in the sidebar
default_start_date = datetime.date(2025, 4, 23)
default_end_date = df["pub_date"].max()


date_min, date_max = df["pub_date"].min(), df["pub_date"].max()
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=[default_start_date, default_end_date],
    min_value=date_min,
    max_value=date_max
)
# New: Add text inputs for keyword filters
# New: Filter by headline keywords (comma-separated list)
headline_keywords = st.sidebar.text_input(
    "Headline keywords (comma-separated)",
    key="headline_keywords"  # New: unique key to avoid duplicate ID
)
# New: Filter by article text keywords (comma-separated list)
article_keywords = st.sidebar.text_area(
    "Article keywords (comma-separated)",
    key="article_keywords"  # New: unique key for the text_area
)

# Filtered data
filtered = df[
    (df["source"].isin(sources)) &
    (df["pub_date"] >= pd.to_datetime(date_range[0])) &
    (df["pub_date"] <= pd.to_datetime(date_range[1]))
]

# New: Apply headline keyword filter
if headline_keywords:
    keywords = [kw.strip() for kw in headline_keywords.split(",") if kw.strip()]
    filtered = filtered[
        filtered["headline"].str.contains("|".join(keywords), case=False, na=False)
    ]
  


# New: Apply article keyword filter
# Requires 'article_text' column; rename if your DataFrame uses a different name
# New: Apply article keyword filter
if article_keywords:
    keywords = [kw.strip() for kw in article_keywords.split(",") if kw.strip()]
    filtered = filtered[
        filtered["article_text"]
            .str
            .contains("|".join(keywords), case=False, na=False)
    ]


print(f"Filtered articles from hyattsville wire : {len(filtered[filtered['source'] == 'Hyattsville Wire'])}")


# 📅 Articles Over Time (Bar Chart, Daily, Side-by-Side)
st.subheader("📅 Articles Over Time (Bar Chart, Daily)")
articles_over_time_daily = (
    filtered.groupby([pd.Grouper(key="pub_date", freq="D"), "source"]).size().reset_index(name="count")
)
fig_time_bar_daily = px.bar(
    articles_over_time_daily,
    x="pub_date",
    y="count",
    color="source",
    barmode="group",
    title="Articles Published Over Time (Daily, Side-by-Side)",
    labels={"pub_date": "Publication Date", "count": "Number of Articles", "source": "News Source"}
)
st.plotly_chart(fig_time_bar_daily, use_container_width=True)

st.subheader("✍️ Headline Length Box Plot")
fig_headline = px.box(
    filtered,
    x="source",
    y="headline_len",
    points="all",
    title="Headline Length per Article",
    labels={"headline_len": "Headline Length", "source": "News Source"}
)
st.plotly_chart(fig_headline, use_container_width=True)

st.subheader("📝 Word Count Box Plot")
fig_word = px.box(
    filtered,
    x="source",
    y="word_count",
    points="all",
    title="Word Count per Article",
    labels={"word_count": "Word Count", "source": "News Source"}
)
st.plotly_chart(fig_word, use_container_width=True)

# IMAGE COUNT EXCLUDED UNTIL WE CAN FIX THE DATA COLLECTED FROM CAPITAL GAZETTE (charts shown in the news articles
# are not picked up as images)

# 🖼️ Image Count Distribution (Excludes Baltimore Banner)
st.subheader("🖼️ Image Count Distribution")
img_note = (
    "**Note:** Baltimore Banner articles are excluded from this chart because image data could not be accurately collected for this source."
)
img_tip = (
    "**Tip:** Change date range to start at 2024-01-01 to see more articles from the Hyattsville Wire."
)
st.markdown(img_note)
st.markdown(img_tip)
img_filtered = filtered[filtered["source"] != "Baltimore Banner"]

# Count number of articles for each num_images value per source
img_counts = img_filtered.groupby(["num_images", "source"]).size().reset_index(name="count")

fig_img = px.bar(
    img_counts,
    x="num_images",
    y="count",
    color="source",
    barmode="group",
    title="Number of Images per Article (Excludes Baltimore Banner)",
    labels={"num_images": "Number of Images", "count": "Number of Articles", "source": "News Source"}
)
st.plotly_chart(fig_img, use_container_width=True)

st.subheader("🔗 Number of Links per Article by Source")
fig_links = px.box(
    filtered,
    x="source",
    y="num_links",
    points="all",
    title="Distribution of Links per Article by News Source",
    labels={"num_links": "Number of Links", "source": "News Source"}
)
st.plotly_chart(fig_links, use_container_width=True)


# 📚 Section Popularity Over Time (Excludes Hyattsville Wire)
st.subheader("📚 Section Popularity Over Time (Line Chart, Daily)")
section_note = (
    "**Note:** Hyattsville Wire is excluded from this chart because its section types differ from the other news sources."
)
st.markdown(section_note)
section_filtered = filtered[filtered["source"] != "Hyattsville Wire"].copy()
section_over_time_daily = (
    section_filtered.groupby([pd.Grouper(key="pub_date", freq="D"), "section"]).size().reset_index(name="count")
)
fig_section_line_daily = px.line(
    section_over_time_daily,
    x="pub_date",
    y="count",
    color="section",
    title="Section Popularity Over Time (Daily, Excludes Hyattsville Wire)",
    labels={"pub_date": "Publication Date", "count": "Number of Articles", "section": "Section"}
)
# Make the lines thicker
fig_section_line_daily.update_traces(line=dict(width=3))

st.plotly_chart(fig_section_line_daily, use_container_width=True)

# 🧮 Average Article Length by Section (Side-by-Side by News Site)
st.subheader("🧮 Average Article Length by Section (by News Site)")

article_length_by_section_note = (
    "**Note:** Hyattsville Wire is excluded from this chart because its section types differ from the other news sources."
)
st.markdown(article_length_by_section_note)
# Combine all sources for a grouped bar chart
avg_lengths_all = (
    filtered[filtered['source'] != "Hyattsville Wire"].groupby(["source", "section"])["word_count"]
    .mean()
    .reset_index()
)
fig_avg_length_grouped = px.bar(
    avg_lengths_all,
    x="section",
    y="word_count",
    color="source",
    barmode="group",
    title="Average Word Count per Section (Grouped by News Site)",
    labels={"word_count": "Average Word Count", "section": "Section", "source": "News Site"},
)
st.plotly_chart(fig_avg_length_grouped, use_container_width=True)

# Visualization: Number of articles by day of the week, separated by source
st.subheader("\U0001F4C6 Articles by Day of the Week (by Source)")

# Add a toggle button for relative/absolute bar chart
show_relative = st.checkbox("Click Here to Show as Percentage (Relative Bar Chart)", value=False)

filtered = filtered.copy()  
filtered["weekday"] = filtered["pub_date"].dt.day_name()

if show_relative:
    # Calculate percentage of articles for each source by weekday (relative to total for that source)
    weekday_counts = filtered.groupby(["source", "weekday"]).size().reset_index(name="count")
    source_totals = weekday_counts.groupby("source")["count"].transform("sum")
    weekday_counts["percent"] = 100 * weekday_counts["count"] / source_totals
    fig_weekday_source = px.bar(
        weekday_counts,
        x="weekday",
        y="percent",
        color="source",
        category_orders={"weekday": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]},
        title="Percentage of Articles by Day of the Week (by Source, Relative to Source Total)",
        labels={"weekday": "Day of Week", "percent": "Percentage of Articles", "source": "News Source"},
        barmode="group"
    )
else:
    fig_weekday_source = px.histogram(
        filtered,
        x="weekday",
        color="source",
        category_orders={"weekday": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]},
        title="Number of Articles by Day of the Week (by Source, Count of articles)",
        labels={"weekday": "Day of Week", "count": "Number of Articles"},
        barmode="group"
    )

st.plotly_chart(fig_weekday_source, use_container_width=True)


# Footer
st.markdown("---")
st.markdown("Data sourced from the Capital Gazette, the Baltimore Banner, and the Hyattsville Wire.")
