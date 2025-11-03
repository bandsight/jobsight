#!/usr/bin/env python3
import os, sys, time, hashlib, re, pandas as pd
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from feedgen.feed import FeedGenerator

MAPPINGS_PATH = Path("council_mappings.csv")
df = pd.read_csv(MAPPINGS_PATH)
COUNCIL_SCRAPERS = [row.to_dict() for _, row in df.iterrows() if pd.notna(row["list_url"])]

HEADERS = {"User-Agent": "JobSight/1.0"}

def fetch_page(url): 
    time.sleep(2)
    return requests.get(url, headers=HEADERS, timeout=15).text

def extract_pay_band(text, default): 
    match = re.search(r'Band\s*([0-9–\-]+)', text or '', re.I)
    return f"Band {match.group(1)}" if match else default

def scrape_council(c):
    jobs = []
    for page in range(1, c["max_pages"] + 1):
        url = f"{c['url']}?page={page}" if page > 1 else c["url"]
        soup = BeautifulSoup(fetch_page(url), "lxml")
        cards = soup.select(c["list_selector"]) if c["list_selector"] else []
        for card in cards:
            title_tag = card.select_one(c["title_sel"])
            if not title_tag: continue
            title = title_tag.get_text(strip=True)
            href = title_tag.get("href", "")
            detail_url = requests.compat.urljoin(url, href) if "{id}" not in c["detail"]["url_pattern"] else c["detail"]["url_pattern"].format(id=href.split("/")[-1])
            det_soup = BeautifulSoup(fetch_page(detail_url), "lxml")
            salary = (det_soup.select_one(c["detail"]["salary"]) or card.select_one(c["salary_sel"])).get_text(strip=True) if det_soup.select_one(c["detail"]["salary"]) or card.select_one(c["salary_sel"]) else ""
            jobs.append({
                "title": title, "council": c["name"], "location": "VIC", "salary": salary,
                "pay_band": extract_pay_band(salary, c["pay_band"]), "closing": "", "url": detail_url,
                "description": "", "published": datetime.utcnow().isoformat()
            })
        if len(cards) < 3: break
    return jobs

def main():
    fg = FeedGenerator()
    feed_path = Path("jobs.xml")
    if feed_path.exists(): fg.load_feed(str(feed_path))
    all_jobs = []
    for c in COUNCIL_SCRAPERS:
        all_jobs.extend(scrape_council(c))
    for job in all_jobs:
        fe = fg.add_entry()
        fe.id(job["url"])
        fe.title(f"{job['title']} – {job['council']}")
        fe.description(f"<![CDATA[<p><strong>Pay Band:</strong> {job['pay_band']}</p>]]>")
    fg.rss_file(str(feed_path))
    print(f"{len(all_jobs)} jobs")

if __name__ == "__main__":
    main()
