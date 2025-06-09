#!/usr/bin/env python3
import time
import json
import logging
from urllib.parse import urlparse, urljoin

import psycopg2
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# — your Neon connection string —
DB_URL = "postgresql://scraperdb_owner:npg_mbyWDf3q5rFp@ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/scraperdb?sslmode=require"

def get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def count_links(page_soup: BeautifulSoup, article_url: str) -> tuple[int,int]:
    """Return (internal_count, external_count)."""
    base_domain = urlparse(article_url).netloc
    anchors = page_soup.select("div.article-body p[data-testid='text-container'] a[href]")
    internal = external = 0
    for a in anchors:
        href = a["href"]
        full = urljoin(article_url, href)
        dom = urlparse(full).netloc
        if dom == "" or dom == base_domain:
            internal += 1
        else:
            external += 1
    return internal, external

def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()
    # 1) Grab every id+url
    cur.execute("SELECT id, url FROM capitol_gazette;")
    rows = cur.fetchall()

    for id_, url in tqdm(rows, desc="Backfilling capitol_gazette "):
        try:
            soup = get_soup(url)
            i_count, e_count = count_links(soup, url)
            # 2) Update the row
            cur.execute(
                """
                UPDATE capitol_gazette
                SET internal_links = %s,
                    external_links = %s
                WHERE id = %s;
                """,
                (i_count, e_count, id_)
            )
        except Exception as exc:
            logging.warning("Failed URL %s: %s", url, exc)
        time.sleep(0.5)  # be gentle on the server

    cur.close()
    conn.close()
    logging.info("Done backfilling all capitol_gazette rows!")

if __name__ == "__main__":
    main()
