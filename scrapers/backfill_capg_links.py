#!/usr/bin/env python3
import time
import logging
from urllib.parse import urlparse, urljoin

import psycopg2
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Your Neon connection string
DB_URL = "postgresql://scraperdb_owner:npg_mbyWDf3q5rFp@" \
         "ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/" \
         "scraperdb?sslmode=require"

# (Optional) mimic a real browser to avoid blocking
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def get_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def count_links(page_soup: BeautifulSoup, article_url: str) -> tuple[int, int]:
    """Return (internal_count, external_count) for a Capital Gazette article."""
    base_domain = urlparse(article_url).netloc

    # ← Fixed selector for Capital Gazette article body
    anchors = page_soup.select("div.body-copy p a[href]")

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

    # 1) Fetch all existing rows
    cur.execute("SELECT id, url FROM capitol_gazette;")
    rows = cur.fetchall()

    # 2) Backfill each
    for id_, url in tqdm(rows, desc="Backfilling capitol_gazette"):
        try:
            soup = get_soup(url)
            i_count, e_count = count_links(soup, url)
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
        time.sleep(0.5)

    cur.close()
    conn.close()
    logging.info("Done backfilling all capitol_gazette rows!")


if __name__ == "__main__":
    main()
