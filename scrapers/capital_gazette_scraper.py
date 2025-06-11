from __future__ import annotations
import re, json, time, logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
import csv
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dt_parser
from io import BytesIO
from tqdm import tqdm
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SECTIONS = {
    "https://www.capitalgazette.com/news/politics/": "politics",
    "https://www.capitalgazette.com/business/": "business",
    "https://www.capitalgazette.com/sports/": "sports",
    "https://www.capitalgazette.com/news/education/": "education",
}

HEADERS = {
    # Chrome-ish UA avoids the site's robots block
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

def load_existing_links(csv_path: str) -> set[str]:
    """Load already scraped article URLs from an existing CSV."""
    if not Path(csv_path).exists():
        return set()
    existing_links = set()
    with Path(csv_path).open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            existing_links.add(row["url"])
    return existing_links


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
                href = href.split("#")[0]  # remove fragments like #comments-header
                if pattern.search(href):
                    links.add(href)
        
        page_url = None  
        time.sleep(0.8)

    return sorted(links)









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
    links_in_body = [a["href"] for p in paragraphs for a in p.find_all("a", href=True)]
    num_links = len(links_in_body)

    # images (count <img> and <svg> tags)
    imgs = soup.select("div.body-copy img")
    svgs = soup.select("div.body-copy svg")
    image_info = []
    for img in imgs:
        src = img.get("src")
        if not src:
            continue
        w = img.get("width")
        h = img.get("height")
        image_info.append({"src": src, "width": w, "height": h, "type": "img"})
    for svg in svgs:
        # For SVGs, we can store the outer HTML as a string for reference
        svg_html = str(svg)
        image_info.append({"src": None, "width": None, "height": None, "type": "svg", "svg_html": svg_html})
    num_images = len(image_info)

    # date
    meta_date = soup.find("meta", attrs={"property": "article:published_time"})
    pub_date = meta_date["content"] if meta_date else None

    # selenium for ad detection
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try:
        driver.get(url)
        time.sleep(3)
        ad_divs = driver.find_elements("css selector", "div[class*='htl-ad']")
        ad_count = len(ad_divs)
    finally:
        driver.quit()

    return {
        "url": url,
        "headline": headline,
        "headline_len": headline_len,
        "pub_date": pub_date,
        "word_count": word_count,
        "num_links": num_links,
        "num_images": num_images,
        "images": image_info,
        "ad_count": ad_count,
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
    cur.execute("SELECT url FROM capital_gazette;")
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

                insert_query = """
                INSERT INTO capital_gazette
                (section, url, pub_date, headline, headline_len,
                 word_count, num_links, num_images, ad_count, images, article_text, date_scraped)
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
                    data.get("ad_count"),
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
    main()   # remove limit when you're satisfied
