import csv, time, random
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

BASE_URL = (
    "https://www.seloger.com/list.htm"
    "?projects=2&types=1"
    "&places=%5B%7Bci%3A750056%7D%5D" 
    "&LISTING-LISTpg={page}"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "fr-FR,fr;q=0.9"
}

def get_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def parse_card(card):
    prix = card.select_one(".c-pa-price").get_text(strip=True).replace("€", "").replace(" ", "")
    surface = card.select_one(".c-pa-m").get_text(strip=True).replace("m²", "")
    adresse = card.select_one(".c-pa-link").get("title")
    url = "https://www.seloger.com" + card.select_one(".c-pa-link")["href"]
    return {
        "prix_euro": prix,
        "surface_m2": surface,
        "adresse": adresse,
        "url": url
    }

def scrape(pages=3):
    results = []
    for p in range(1, pages + 1):
        url = BASE_URL.format(page=p)
        print("▶ Scraping", url)
        soup = get_soup(url)
        cards = soup.select("li.c-pa-list li.c-pa-item")
        if not cards:
            break
        for c in cards:
            try:
                results.append(parse_card(c))
            except Exception as e:
                print("parse error:", e)
        time.sleep(random.uniform(1, 2))
    return results

def save_csv(data):
    out = Path("data") / f"seloger_listings_{datetime.today():%Y%m%d}.csv"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=data[0].keys())
        w.writeheader()
        w.writerows(data)
    print(f"✅ {len(data)} annonces sauvegardées dans {out}")

if __name__ == "__main__":
    ads = scrape(pages=2)
    if ads:
        save_csv(ads)
    else:
        print("⚠️ Aucun résultat récupéré.")