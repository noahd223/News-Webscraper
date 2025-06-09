# capital_gazette_scraper.py
from __future__ import annotations
import re, json, time, logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import csv
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dt_parser
from PIL import Image
from io import BytesIO
from tqdm import tqdm
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

SECTIONS = {
    "https://www.capitalgazette.com/news/politics/": "politics",
    "https://www.capitalgazette.com/business/": "business",
    "https://www.capitalgazette.com/sports/": "sports",
    "https://www.capitalgazette.com/news/education/": "education",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ------------- helpers ----------------------------------------------------- #
def get_soup(url: str) -> BeautifulSoup:
    resp = SESSION.get(url, timeout=20)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def get_all_page_links(section_url: str, label: str) -> list[str]:
    """Collect ONLY real article links from a section page based on section type."""
    links: set[str] = set()
    page_url = section_url

    pattern = re.compile(
        r"^https://www\.capitalgazette\.com/\d{4}/\d{2}/\d{2}/[^/]+/$"
    )

    while page_url:
        soup = get_soup(page_url)
        for a in soup.select("a[href]"):
            href = a["href"]
            if href.startswith("/"):
                href = urljoin(section_url, href)
            if href.startswith("https://www.capitalgazette.com/"):
                href = href.split("#")[0]
                if pattern.search(href):
                    links.add(href)
        # only one page for Capital Gazette
        page_url = None
        time.sleep(0.8)

    return sorted(links)


def get_image_dims(src: str) -> tuple[int | None, int | None]:
    try:
        r = SESSION.get(src, timeout=15)
        r.raise_for_status()
        with Image.open(BytesIO(r.content)) as im:
            return im.width, im.height
    except Exception:
        return None, None


# ------------- article extractor ------------------------------------------ #
def parse_article(url: str) -> dict:
    soup = get_soup(url)

    # headline
    headline_tag = soup.select_one("h1.entry-title")
    headline = headline_tag.get_text(strip=True) if headline_tag else ""
    headline_len = len(headline.split())

    # word count
    paragraphs = soup.select("div.body-copy p")
    text = " ".join(p.get_text(strip=True) for p in paragraphs)
    word_count = len(text.split())

    # links
    links_in_body = [
        a["href"]
        for p in paragraphs
        for a in p.find_all("a", href=True)
    ]
    num_links = len(links_in_body)

    # NEW: split into internal vs external
    parsed_base = urlparse(url)
    base_domain = parsed_base.netloc
    internal_links = 0
    external_links = 0
    for href in links_in_body:
        full_url = urljoin(url, href)
        dom = urlparse(full_url).netloc
        if dom == "" or dom == base_domain:
            internal_links += 1
        else:
            external_links += 1

    # images (img + svg)
    imgs = soup.select("div.body-copy img")
    svgs = soup.select("div.body-copy svg")
    image_info = []
    for img in imgs:
        src = img.get("src")
        if not src:
            continue
        w = img.get("width")
        h = img.get("height")
        if not (w and h):
            w, h = get_image_dims(src)
        image_info.append({"src": src, "width": w, "height": h, "type": "img"})
    for svg in svgs:
        svg_html = str(svg)
        image_info.append({"src": None, "width": None, "height": None,
                           "type": "svg", "svg_html": svg_html})
    num_images = len(image_info)

    # date
    meta_date = soup.find("meta", attrs={"property": "article:published_time"})
    pub_date = meta_date["content"] if meta_date else None

    # ads estimate
    ad_count = len(soup.select("div[id^='arcad-feature']"))

    return {
        "url": url,
        "headline": headline,
        "headline_len": headline_len,
        "pub_date": pub_date,
        "word_count": word_count,
        "num_links": num_links,
        "internal_links": internal_links,   # NEW
        "external_links": external_links,   # NEW
        "num_images": num_images,
        "images": image_info,
        "num_ads_est": ad_count,
        "article_text": text,
    }


# ------------- main -------------------------------------------------------- #
def main(limit_per_section: int | None = None):
    conn = psycopg2.connect(
        "postgresql://scraperdb_owner:npg_mbyWDf3q5rFp@"
        "ep-still-snowflake-a4l5opga-pooler.us-east-1.aws.neon.tech/"
        "scraperdb?sslmode=require"
    )
    conn.autocommit = True
    cur = conn.cursor()

    existing_urls = set()
    cur.execute("SELECT url FROM capitol_gazette;")
    for row in cur.fetchall():
        existing_urls.add(row[0])

    new_articles_count = 0
    for section_url, label in SECTIONS.items():
        logging.info("Scanning section: %s", label)
        article_links = get_all_page_links(section_url, label)
        if limit_per_section:
            article_links = article_links[:limit_per_section]

        for url in tqdm(article_links, desc=f"{label:>10}", unit="article"):
            if url in existing_urls:
                logging.info("Already scraped: %s", url)
                continue

            try:
                data = parse_article(url)
                data["section"] = label

                # ─── INSERT WITH INTERNAL/EXTERNAL LINKS ─────────────────
                insert_query = """
                INSERT INTO capitol_gazette
                (section, url, pub_date, headline, headline_len,
                 word_count, num_links, internal_links, external_links,
                 num_images, num_ads_est, images, article_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    data.get("internal_links"),   # NEW
                    data.get("external_links"),   # NEW
                    data.get("num_images"),
                    data.get("num_ads_est"),
                    json.dumps(data.get("images")),
                    data.get("article_text"),
                ))
                # ──────────────────────────────────────────────────────────

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
    # ─── DEBUG TEST for internal/external link counting ────────────────
    # Pick a real Capital Gazette article URL that you know exists:
    test_url = "https://www.capitalgazette.com/2025/04/28/anne-arundel-special-tax-districts-boards-deny-secrecy-overspending-claims/"
    data = parse_article(test_url)
    print("URL:             ", data["url"])
    print("Total links:     ", data["num_links"])
    print("Internal links:  ", data["internal_links"])
    print("External links:  ", data["external_links"])
    import sys; sys.exit(0)
#if __name__ == "__main__":
   # main()  # remove limit when you’re satisfied
 
