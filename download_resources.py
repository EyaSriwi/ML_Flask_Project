# download_resources.py
import os
import json
import time
import re
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# ----- CONFIG -----
OUTPUT_DIR = "OUTPUT"
MD_FOLDER = os.path.join(OUTPUT_DIR, "markdowns")
IMG_FOLDER = os.path.join(OUTPUT_DIR, "images")
DOC_FOLDER = os.path.join(OUTPUT_DIR, "documentation")
TABLES_FOLDER = os.path.join(OUTPUT_DIR, "tables")
OTHER_FOLDER = os.path.join(OUTPUT_DIR, "other")

# Extensions mapping for file downloads
FILE_EXTS = {
    "documentation": ["pdf", "xlsx", "xls", "ppt", "pptx", "doc", "docx"],
    "images": ["jpg", "jpeg", "png", "gif", "svg", "webp"],
    "design_resources": ["cad", "zip", "rar"],
    "software_tools": ["py", "cpp", "zip", "exe"],
    "tables": ["csv", "json", "xls", "xlsx"],
    "trainings": ["mp4", "mov", "mkv"]
}

# Ensure base folders exist
for d in (MD_FOLDER, IMG_FOLDER, DOC_FOLDER, TABLES_FOLDER, OTHER_FOLDER):
    os.makedirs(d, exist_ok=True)

# Retry-capable requests session
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2)
session.mount("https://", adapter)
session.mount("http://", adapter)
HEADERS = {"User-Agent": "inspur-scraper/1.0 (+contact)"}

# Helpers
def safe_name(name):
    name = re.sub(r"[\/\\\:\*\?\"<>\|]", "_", name)
    name = re.sub(r"\s+", "_", name).strip("_")
    return name[:120] if name else "topic"

def clean_url(u):
    if not u:
        return ""
    return u.strip().replace("%20", "").rstrip("/")

def is_file_url(u):
    path = urlparse(u).path
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    return ext in {e for exts in FILE_EXTS.values() for e in exts}

def choose_folder_by_ext(url):
    path = urlparse(url).path
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    for folder, exts in FILE_EXTS.items():
        if ext in exts:
            return folder
    return "other"

def download_binary(url, out_folder):
    try:
        os.makedirs(out_folder, exist_ok=True)
        filename = os.path.basename(urlparse(url).path) or "file"
        filename = filename.split("?")[0]
        filename = safe_name(filename)
        path = os.path.join(out_folder, filename)
        if os.path.exists(path):
            return filename, path, "exists"
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        return filename, path, "downloaded"
    except Exception as e:
        print(f"  ✖ Failed to download {url}: {e}")
        return None, None, "failed"

def update_metadata_file(folder_rel, entry):
    metadata_file = os.path.join(OUTPUT_DIR, folder_rel, "metadata.json")
    os.makedirs(os.path.join(OUTPUT_DIR, folder_rel), exist_ok=True)
    data = []
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except:
            data = []
    data.append(entry)
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# Load topics
ts_path = os.path.join(OUTPUT_DIR, "topic_structure.json")
with open(ts_path, "r", encoding="utf-8") as f:
    topics = json.load(f)

# Ensure URLs cleaned recursively
def clean_topics(topics_list):
    for t in topics_list:
        if "url" in t and t["url"]:
            t["url"] = clean_url(t["url"])
        if "sub_topics" in t and t["sub_topics"]:
            clean_topics(t["sub_topics"])

clean_topics(topics)

# Accumulate products table (basic)
products = {}

# Main processing
def process_topics(topics_list, parent_url=None):
    for t in topics_list:
        name = t.get("name", "unnamed")
        url = t.get("url") or (parent_url + "#" + safe_name(name) if parent_url else None)
        if not url or not url.startswith("http"):
            print(f"Skipping invalid URL: {url} ({name})")
            if t.get("sub_topics"):
                process_topics(t["sub_topics"], parent_url=url if parent_url else None)
            continue

        print(f"Processing topic: {name} -> {url}")
        # quick HEAD to check
        try:
            head = session.head(url, headers=HEADERS, timeout=8, allow_redirects=True)
            if head.status_code >= 400:
                print(f"  URL not reachable (skipping): {url} (status {head.status_code})")
                if t.get("sub_topics"):
                    process_topics(t["sub_topics"], parent_url=url)
                continue
        except Exception:
            # try GET (some sites block HEAD)
            try:
                rtmp = session.get(url, headers=HEADERS, timeout=12)
                rtmp.raise_for_status()
            except Exception as e:
                print(f"  Cannot reach URL (skipping): {url} ({e})")
                if t.get("sub_topics"):
                    process_topics(t["sub_topics"], parent_url=url)
                continue

        # GET page
        try:
            r = session.get(url, headers=HEADERS, timeout=12)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            print(f"  Error fetching {url}: {e}")
            if t.get("sub_topics"):
                process_topics(t["sub_topics"], parent_url=url)
            continue

        # --- 1) Save page content as Markdown ---
        try:
            page_md = md(str(soup.find("main") or soup.body or soup), heading_style="ATX")
            md_name = safe_name(name)
            md_path = os.path.join(MD_FOLDER, f"{md_name}.md")
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(f"# {name}\n\n")
                f.write(f"**Source:** {url}\n\n")
                f.write(page_md)
            # metadata for markdown
            update_metadata_file("markdowns", {
                "name": f"{md_name}.md",
                "file_path": os.path.relpath(md_path, OUTPUT_DIR).replace("\\", "/"),
                "version": None,
                "date": None,
                "url": url,
                "language": None,
                "description": None
            })
            print(f"  📝 Saved markdown: {md_name}.md")
        except Exception as e:
            print(f"  ✖ Failed to save markdown for {url}: {e}")

        # --- 2) Extract & download images ---
        imgs = []
        for img in soup.find_all("img", src=True):
            src = img.get("src")
            if not src:
                continue
            img_url = src
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            elif img_url.startswith("/"):
                base = "{scheme}://{netloc}".format(scheme=urlparse(url).scheme, netloc=urlparse(url).netloc)
                img_url = urljoin(base, img_url)
            elif not img_url.startswith("http"):
                img_url = urljoin(url, img_url)
            imgs.append(img_url)
        imgs = list(dict.fromkeys(imgs))  # unique

        for img_url in imgs:
            fname, path, status = download_binary(img_url, IMG_FOLDER)
            if status in ("downloaded", "exists"):
                update_metadata_file("images", {
                    "name": fname,
                    "file_path": os.path.relpath(path, OUTPUT_DIR).replace("\\", "/"),
                    "version": None,
                    "date": None,
                    "url": img_url,
                    "language": None,
                    "description": None
                })

        if imgs:
            print(f"  🖼 {len(imgs)} images processed")

        # --- 3) Find downloadable file links (pdf, doc, zip...) and download them ---
        downloadable_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href").strip()
            if href.startswith("javascript:") or href.startswith("#") or href.lower().startswith("mailto:"):
                continue
            if href.startswith("/"):
                href = urljoin(f"{urlparse(url).scheme}://{urlparse(url).netloc}", href)
            elif not href.startswith("http"):
                href = urljoin(url, href)
            if is_file_url(href):
                downloadable_links.add(href)

        for file_url in downloadable_links:
            folder_key = choose_folder_by_ext(file_url)
            # map folder_key to real path
            target_folder = DOC_FOLDER if folder_key == "documentation" else (
                TABLES_FOLDER if folder_key == "tables" else
                IMG_FOLDER if folder_key == "images" else OTHER_FOLDER
            )
            fname, path, status = download_binary(file_url, target_folder)
            if status in ("downloaded", "exists"):
                rel_folder = os.path.relpath(os.path.dirname(path), OUTPUT_DIR).replace("\\", "/")
                update_metadata_file(rel_folder, {
                    "name": fname,
                    "file_path": os.path.relpath(path, OUTPUT_DIR).replace("\\", "/"),
                    "version": None,
                    "date": None,
                    "url": file_url,
                    "language": None,
                    "description": None
                })
        if downloadable_links:
            print(f"  📦 {len(downloadable_links)} file(s) downloaded")

        # --- 4) Collect product entry (basic) to products.json ---
        title_text = soup.find("h1").get_text(strip=True) if soup.find("h1") else name
        summary = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p")[:5])
        product_key = safe_name(title_text)[:60]
        products[product_key] = {
            "Product": title_text,
            "Status": None,
            "Pricing": None,
            "Topology": None,
            "description": summary or None,
            "summary": summary or None,
            "pdf_link": next(iter(downloadable_links), None),
            "pdf_filename": None,
            "product_page_link": url,
            "image_url": imgs[0] if imgs else None
        }

        # small politeness delay
        time.sleep(0.3)

        # Recurse into subtopics
        if t.get("sub_topics"):
            process_topics(t["sub_topics"], parent_url=url)

# Run
process_topics(topics)

# Write products.json
products_json_path = os.path.join(TABLES_FOLDER, "products.json")
os.makedirs(TABLES_FOLDER, exist_ok=True)
with open(products_json_path, "w", encoding="utf-8") as f:
    json.dump(products, f, indent=2, ensure_ascii=False)
print(f"\n✅ Scraping finished! Products saved: {products_json_path}")
