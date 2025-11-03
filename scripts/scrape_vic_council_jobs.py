#!/usr/bin/env python3
import os, sys, time, hashlib, re, pandas as pd
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from feedgen.feed import FeedGenerator

MAPPINGS_PATH = Path("council_mappings.csv")
if not MAPPINGS_PATH.exists():
    print("ERROR: council_mappings.csv missing!")
    sys.exit(1)

df = pd.read_csv(MAPPINGS_PATH)
COUNCIL_SCRAPERS = []
for _, row in df.iterrows():
    if pd.isna(row.get("list_url")):
        continue
    scraper = {
        "council": row["council"],
        "url": row["list_url"].strip(),
        "max_pages": int(row.get("max_pages", 5)),
        "list_selector": row.get("list_selector", "").strip(),
        "title_sel": row["title_sel"].strip(),
        "location_sel": row.get("location_sel", ""),
        "salary_sel": row.get("salary_sel", ""),
        "closing_sel": row.get("closing_sel", ""),
        "detail_url_pattern": row.get("detail_url_pattern", "{href}"),
        "detail_location": row.get("detail_location", ""),
        "detail_salary": row.get("detail_salary", ""),
        "detail_closing": row.get("detail_closing", ""),
        "detail_description": row.get("detail_description", ""),
        "pay_band": row.get("pay_band", "Not specified"),
    }
    COUNCIL_SCRAPERS.append(scraper)

HEADERS = {"User-Agent": "JobSight/1.0 (+https://github.com/bandsight/jobsight)"}

def fetch_page(url):
    try:
        time.sleep(2)
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"Fetch failed {url}: {e}")
        return ""

def safe_text(tag):
    return tag.get_text(strip=True) if tag else ""

def extract_pay_band(text, default):
    if not text: return default
    match = re.search(r'Band\s*([0-9–\-]+)', text, re.I)
    return f"Band {match.group(1)}" if match else default

def build_detail_url(base, href, pattern):
    if pattern == "{href}":
        return requests.compat.urljoin(base, href)
    if "{id}" in pattern:
        job_id = href.split("=")[-1] if "=" in href else href.split("/")[-1]
        return pattern.format(id=job_id)
    return pattern.format(href=href)

def scrape_council(c):
    jobs = []
    seen = set()
    base_url = c["url"]
    for page in range(1, c["max_pages"] + 1):
        url = f"{base_url}?page={page}" if page > 1 else base_url
        html = fetch_page(url)
        if not html: 
            print(f"No HTML for {url}")
            break
        soup = BeautifulSoup(html, "lxml")
        if not c["list_selector"]:
            print(f"No list_selector for {c['council']}")
            continue
        cards = soup.select(c["list_selector"])
        if not cards: 
            print(f"No cards on page {page}")
            break

        for card in cards:
            title_tag = card.select_one(c["title_sel"])
            if not title_tag: continue
            title = safe_text(title_tag)
            href = title_tag.get("href", "")
            if not href: continue
            detail_url = build_detail_url(url, href, c["detail_url_pattern"])
            if detail_url in seen: continue
            seen.add(detail_url)

            det_html = fetch_page(detail_url)
            if not det_html: continue
            det_soup = BeautifulSoup(det_html, "lxml")

            salary_tag = det_soup.select_one(c["detail_salary"]) or card.select_one(c["salary_sel"])
            salary = safe_text(salary_tag)
            pay_band = extract_pay_band(salary, c["pay_band"])

            location = safe_text(det_soup.select_one(c["detail_location"]) or card.select_one(c["location_sel"])) or "VIC"
            description = safe_text(det_soup.select_one(c["detail_description"]))[:1000]

            jobs.append({
                "title": title,
                "council": c["council"],
                "location": location,
                "salary": salary,
                "pay_band": pay_band,
                "closing": "",
                "url": detail_url,
                "description": description,
                "published": datetime.utcnow().isoformat()
            })
        if len(cards) < 5: break
    print(f"{c['council']}: {len(jobs)} jobs scraped")
    return jobs

def main():
    fg = FeedGenerator()
    feed_path = Path("jobs.xml")
    existing = set()
    if feed_path.exists():
        try:
            fg.load_feed(str(feed_path))
            for e in fg.entry():
                existing.add(e.id())
        except Exception as e:
            print(f"Feed load error: {e} — starting fresh")

    fg.id("https://github.com/bandsight/jobsight")
    fg.title("JobSight: Victorian Council Jobs")
    fg.description("78 councils • Pay bands 1-8 • Updated every 6 hours")
    fg.link(href="https://github.com/bandsight/jobsight", rel="alternate")
    fg.language("en")

    all_jobs = []
    for c in COUNCIL_SCRAPERS:
        try:
            all_jobs.extend(scrape_council(c))
        except Exception as e:
            print(f"Error scraping {c['council']}: {e}")

    new_jobs = [j for j in all_jobs if j["url"] not in existing]
    for job in sorted(new_jobs, key=lambda x: x["published"], reverse=True):
        fe = fg.add_entry()
        fe.id(job["url"])
        fe.title(f"{job['title']} – {job['council']}")
        fe.link(href=job["url"])
        fe.description(f"<![CDATA["
                       f"<p><strong>Location:</strong> {job['location']}</p>"
                       f"<p><strong>Salary:</strong> {job['salary']}</p>"
                       f"<p><strong>Pay Band:</strong> {job['pay_band']}</p>"
                       f"<hr>{job['description']}"
                       f"]]>")
        fe.published(job["published"])

    fg.rss_file(str(feed_path))
    print(f"SUCCESS: {len(new_jobs)} new jobs → {len(all_jobs)} total in feed")

if __name__ == "__main__":
    main()
