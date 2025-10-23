import requests
from bs4 import BeautifulSoup
import os
import json
import re
from markdownify import markdownify as md

OUTPUT_DIR = "output"
crawled_urls = set()  # Track already crawled URLs

def sanitize_filename(name):
    """Sanitize string to be a safe filename."""
    name = " ".join(name.strip().split())  # remove extra spaces
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    if not name:
        name = "default"
    return name

def save_file(url, folder):
    """Download a file from URL and save it to the specified folder."""
    os.makedirs(folder, exist_ok=True)
    url = url.strip().replace(" ", "")
    if url.startswith("javascript:") or not url:
        return None
    filename = url.rstrip("/").split("/")[-1].split("?")[0]
    filename = sanitize_filename(filename)
    if not filename:
        filename = "index.html"
    filepath = os.path.join(folder, filename)
    
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
        print(f"Downloaded: {filename}")
        return filepath
    except Exception as e:
        print(f"Erreur téléchargement {url}: {e}")
        return None

def crawl_topic(topic):
    """Crawl a topic page, save HTML as Markdown, and download PDFs."""
    name = sanitize_filename(topic['name'])
    url = topic["url"].strip().replace(" ", "")
    
    # Skip duplicates and placeholders
    if url in crawled_urls or not url or name.lower() == "more":
        return
    crawled_urls.add(url)
    
    print(f"Processing topic: {name}")
    
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erreur de connexion pour {url}: {e}")
        return
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Convert HTML to Markdown
    md_folder = os.path.join(OUTPUT_DIR, "markdowns")
    os.makedirs(md_folder, exist_ok=True)
    md_file = os.path.join(md_folder, f"{name}.md")
    with open(md_file, "w", encoding="utf-8") as f:
        f.write(md(str(soup)))
    
    # Download all PDFs
    doc_folder = os.path.join(OUTPUT_DIR, "documentation")
    for a in soup.select("a[href$='.pdf']"):
        file_url = a.get("href")
        if not file_url.startswith("http"):
            file_url = url.rstrip("/") + "/" + file_url.lstrip("/")
        save_file(file_url, doc_folder)

if __name__ == "__main__":
    # Load topic structure
    with open(os.path.join(OUTPUT_DIR, "topic_structure.json"), encoding="utf-8") as f:
        topics = json.load(f)
    
    for t in topics:
        crawl_topic(t)
    
    print("✅ Crawling finished!")
