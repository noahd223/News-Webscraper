import pandas as pd
from sqlalchemy import create_engine, text

# Load cleaned CSV
df = pd.read_csv("capitol_gazette_labeled.csv")

# Drop misnamed column and rename properly
df = df.drop(columns=[col for col in df.columns if col.lower() == "headline_type" and col != "headline_type"], errors="ignore")
if "Headline_type" in df.columns:
    df = df.rename(columns={"Headline_type": "headline_type"})

# Drop rows with missing headline or headline_type
df = df.dropna(subset=["headline", "headline_type"])

# Replace 'statement' with 'explanatory' in the 'headline_type' column
df['headline_type'] = df['headline_type'].replace('statement', 'explanatory')

# Connect to DB
engine = create_engine(
    "postgresql+psycopg2://scraperdb_owner:npg_mbyWDf3q5rFp@ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/scraperdb?sslmode=require"
)
print(f"columns: {df.columns}")
# Update records
update_query = """
    UPDATE capitol_gazette
    SET headline_type = :headline_type
    WHERE headline = :headline;
"""
print(f"headline_type: {df['headline_type']}")
with engine.begin() as conn:
    for _, row in df.iterrows():
        conn.execute(text(update_query), {
            "headline": row["headline"],
            "headline_type": row["headline_type"]
        })
        print(f"Updated headline: {row['headline']} with type: {row['headline_type']}")

print("✅ PostgreSQL 'headline_type' column updated successfully.")