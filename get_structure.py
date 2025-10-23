# get_structure.py
import requests
from bs4 import BeautifulSoup
import json
import os

BASE_URL = "https://en.inspur.com/"

def get_topics(url):
    """
    Récupère les topics principaux du site Inspur.
    """
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    
    topics = []
    # Récupération simple : tous les liens visibles sur la page
    for a in soup.select("a"):
        name = a.get_text(strip=True)
        link = a.get("href")
        if name and link:
            if not link.startswith("http"):
                link = BASE_URL + link.lstrip("/")
            topics.append({
                "name": name,
                "url": link,
                "breadcrumbs": ["Products", name],
                "sub_topics": []  # Pour l'instant vide, peut être enrichi
            })
    return topics

if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)
    
    # Générer la structure des topics
    structure = get_topics(BASE_URL)
    
    # Sauvegarder dans topic_structure.json
    with open("output/topic_structure.json", "w", encoding="utf-8") as f:
        json.dump(structure, f, indent=4, ensure_ascii=False)
    
    print("✅ Topic structure saved in output/topic_structure.json")
