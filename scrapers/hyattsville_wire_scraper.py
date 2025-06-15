from __future__ import annotations
import re, json, time, itertools, logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import csv
import requests
from bs4 import BeautifulSoup, Tag
from dateutil import parser as dt_parser
from PIL import Image
from io import BytesIO
from tqdm import tqdm
import psycopg2


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/15.1 Safari/605.1.15"
    )
}


SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ------------- helpers ----------------------------------------------------- #
def get_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def get_all_page_links(page_url: str) -> list[str]:
    """Collect article links from a Hyattsville Wire paginated page."""
    links: set[str] = set()
    soup = get_soup(page_url)
    # The article links are in <h2 class='post-title entry-title'> <a href=...>
    for h2 in soup.select("h2.post-title.entry-title"):
        a = h2.find("a", href=True)
        if a and a["href"].startswith("https://hyattsvillewire.com/"):
            links.add(a["href"].split("#")[0])
    time.sleep(0.8)
    return sorted(links)


def get_image_dims(src: str) -> tuple[int | None, int | None]:
    """Return (width, height) or (None, None) if not obtainable quickly."""
    try:
        r = SESSION.get(src, timeout=15)
        r.raise_for_status()
        with Image.open(BytesIO(r.content)) as im:
            return im.width, im.height
    except Exception:  # noqa: BLE001
        return None, None


# ------------- article extractor ------------------------------------------ #
def parse_article(url: str) -> dict:
    soup = get_soup(url)

    # headline
    headline_tag = soup.select_one("h1.entry-title")
    headline = headline_tag.get_text(strip=True) if headline_tag else ""
    headline_len = len(headline.split())

    # content
    paragraphs = soup.select("div.entry-content p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs)
    word_count = len(text.split())

    # links
    links_in_body = [a["href"] for p in paragraphs for a in p.find_all("a", href=True)]
    num_links = len(links_in_body)

    # images
    imgs = soup.select("div.entry-content img")
    image_info = []
    for img in imgs:
        src = img.get("src")
        if not src:
            continue
        w = img.get("width")
        h = img.get("height")
        if not (w and h):
            w, h = get_image_dims(src)
        image_info.append({"src": src, "width": w, "height": h})
    num_images = len(image_info)

    # publication date
    meta_date = soup.find("meta", attrs={"property": "article:published_time"})
    pub_date = meta_date["content"] if meta_date else None

    # ads (optional placeholder)
    ad_count = 0

    return {
        "url": url,
        "headline": headline,
        "headline_len": headline_len,
        "pub_date": pub_date,
        "word_count": word_count,
        "num_links": num_links,
        "num_images": num_images,
        "images": image_info,
        "num_ads_est": ad_count,
        "text": text,
        "date_scraped": datetime.utcnow().isoformat(),
    }


# ------------- main -------------------------------------------------------- #
def main(
    limit_per_section: int | None = None,
):
    conn = psycopg2.connect(
        "postgresql://scraperdb_owner:npg_mbyWDf3q5rFp@ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/scraperdb?sslmode=require"
    )
    conn.autocommit = True
    cur = conn.cursor()

    existing_urls = set()
    cur.execute("SELECT url FROM hyattsville_wire;")
    for row in cur.fetchall():
        existing_urls.add(row[0])

    new_articles_count = 0
    # Scan pages 1 to 100
    for page_num in range(10, 40):
        if page_num == 1:
            page_url = "https://hyattsvillewire.com/"
        else:
            page_url = f"https://hyattsvillewire.com/page/{page_num}/"
        logging.info(f"Scanning page: {page_url}")
        article_links = get_all_page_links(page_url)
        if limit_per_section:
            article_links = article_links[:limit_per_section]

        for url in tqdm(article_links, desc=f"page {page_num:>3}", unit="article"):
            if url in existing_urls:
                logging.info("Already scraped: %s", url)
                continue

            try:
                data = parse_article(url)
                # Set section to a default value (e.g., 'unknown') to avoid NOT NULL violation
                data["section"] = "unknown"

                insert_query = """
                INSERT INTO hyattsville_wire
                (section, url, pub_date, headline, headline_len,
                word_count, num_links, num_images, num_ads_est, images, text, date_scraped)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
                """

                cur.execute(insert_query, (
                    data.get("section"),
                    data.get("url"),
                    data.get("pub_date"),
                    data.get("headline"),
                    data.get("headline_len"),
                    data.get("word_count"),
                    data.get("num_links"),
                    data.get("num_images"),
                    data.get("num_ads_est"),
                    json.dumps(data.get("images")),
                    data.get("text"),
                    data.get("date_scraped"),
                ))

                existing_urls.add(url)
                new_articles_count += 1
            except Exception as exc:
                logging.warning("Failed %s: %s", url, exc)
            time.sleep(0.6)

    cur.close()
    conn.close()
    logging.info("DONE – wrote to Neon database.")
    print(f"New articles scraped: {new_articles_count}")



if __name__ == "__main__":
    main()
