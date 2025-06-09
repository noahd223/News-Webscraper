#!/usr/bin/env python3
# Backfill script for Hyattsville Wire internal/external link counts
import time
import logging
from urllib.parse import urlparse, urljoin

import psycopg2
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Neon connection string
DB_URL = (
    "postgresql://scraperdb_owner:npg_mbyWDf3q5rFp@"
    "ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/"
    "scraperdb?sslmode=require"
)

# Browser-like headers to avoid being blocked
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/15.1 Safari/605.1.15"
    )
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def get_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def count_links(page_soup: BeautifulSoup, article_url: str) -> tuple[int, int]:
    """Return (internal_count, external_count) for a Hyattsville Wire article."""
    base_domain = urlparse(article_url).netloc
    anchors = page_soup.select("div.entry-content p a[href]")
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

    # Fetch all existing article IDs and URLs
    cur.execute("SELECT id, url FROM hyattsville_wire;")
    rows = cur.fetchall()

    # Backfill loop
    for id_, url in tqdm(rows, desc="Backfilling hyattsville_wire"):
        try:
            soup = get_soup(url)
            i_count, e_count = count_links(soup, url)
            cur.execute(
                """
                UPDATE hyattsville_wire
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
    logging.info("Done backfilling all hyattsville_wire rows!")


if __name__ == "__main__":
    main()
