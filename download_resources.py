import os
import json
import requests
from bs4 import BeautifulSoup

# Folder mapping by file extension
FOLDER_MAPPING = {
    "documentation": ["pdf", "xlsx", "ppt", "doc", "docx"],
    "images": ["jpg", "jpeg", "png", "gif", "svg"],
    "block_diagrams": ["pdf", "png", "jpeg"],
    "design_resources": ["cad", "zip"],
    "software_tools": ["cpp", "py", "zip", "exe"],
    "tables": ["csv", "json", "xls"],
    "trainings": ["mp4", "mov", "pdf", "md"],
    "other": []
}

OUTPUT_DIR = "OUTPUT"

# Load topic structure
with open(os.path.join(OUTPUT_DIR, "topic_structure.json"), "r", encoding="utf-8") as f:
    topics = json.load(f)

# Clean URLs (strip spaces, trailing slashes)
def clean_urls(topics):
    for t in topics:
        if "url" in t and t["url"]:
            t["url"] = t["url"].strip().rstrip("/")
        if "sub_topics" in t and t["sub_topics"]:
            clean_urls(t["sub_topics"])
    return topics

topics = clean_urls(topics)

# Utility: Download file
def download_file(url, folder):
    os.makedirs(folder, exist_ok=True)
    filename = url.split("/")[-1].split("?")[0] or "file"
    path = os.path.join(folder, filename)
    if os.path.exists(path):
        print(f"Already exists: {filename}")
        return filename, path
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        with open(path, "wb") as f:
            f.write(r.content)
        print(f"Downloaded: {filename}")
        return filename, path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None, None

# Determine folder based on file extension
def get_folder_for_file(filename):
    ext = filename.split(".")[-1].lower()
    for folder, exts in FOLDER_MAPPING.items():
        if ext in exts:
            return folder
    return "other"

# Update metadata.json
def update_metadata(folder, name, path, url):
    metadata_file = os.path.join(OUTPUT_DIR, folder, "metadata.json")
    os.makedirs(os.path.join(OUTPUT_DIR, folder), exist_ok=True)
    
    metadata = []
    if os.path.exists(metadata_file):
        with open(metadata_file, "r", encoding="utf-8") as f:
            try:
                metadata = json.load(f)
            except:
                metadata = []

    metadata.append({
        "name": name,
        "file_path": os.path.relpath(path, OUTPUT_DIR).replace("\\","/"),
        "version": "1.0.0",
        "date": "2025-10-23",
        "url": url,
        "language": "english",
        "description": None
    })

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

# Recursive function to process topics
def process_topics(topics):
    for t in topics:
        name = t.get("name")
        url = t.get("url")
        if not url or not url.startswith("http") or "javascript:void" in url:
            print(f"Skipping invalid URL: {url}")
            continue

        # Optional: skip non-existing URLs
        try:
            r_head = requests.head(url, timeout=5)
            if r_head.status_code >= 400:
                print(f"URL not reachable (skipping): {url}")
                continue
        except:
            print(f"Cannot reach URL (skipping): {url}")
            continue

        print(f"Processing topic: {name} -> {url}")

        # Fetch page content
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Find all links to downloadable files
            links = soup.find_all("a", href=True)
            for link in links:
                file_url = link["href"].strip()
                if file_url.startswith("http"):
                    file_ext = file_url.split(".")[-1].lower()
                    if any(file_ext == ext for exts in FOLDER_MAPPING.values() for ext in exts):
                        folder = get_folder_for_file(file_url)
                        fname, path = download_file(file_url, os.path.join(OUTPUT_DIR, folder))
                        if fname and path:
                            update_metadata(folder, fname, path, file_url)

        except Exception as e:
            print(f"Error fetching {url}: {e}")

        # Recursively process subtopics
        if t.get("sub_topics"):
            process_topics(t["sub_topics"])

# Start processing
process_topics(topics)
print("✅ Scraping finished!")
