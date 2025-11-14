import csv
import random
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from backend.logging_setup import setup_logging

logger = setup_logging()

BASE_URL = (
    "https://www.seloger.com/list.htm"
    "?projects=2&types=1"
    "&places=%5B%7Bci%3A750056%7D%5D"
    "&LISTING-LISTpg={page}"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


def get_soup(url):
    """Récupère et parse une page HTML."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        logger.error("Erreur lors du téléchargement de %s : %s", url, e)
        raise


def parse_card(card):
    """Extrait les informations clés d'une carte d’annonce."""
    try:
        prix = (
            card.select_one(".c-pa-price")
            .get_text(strip=True)
            .replace("€", "")
            .replace(" ", "")
        )
        surface = card.select_one(".c-pa-m").get_text(strip=True).replace("m²", "")
        adresse = card.select_one(".c-pa-link").get("title")
        url = "https://www.seloger.com" + card.select_one(".c-pa-link")["href"]
        return {
            "prix_euro": prix,
            "surface_m2": surface,
            "adresse": adresse,
            "url": url,
        }
    except Exception as e:
        logger.warning("Erreur lors du parsing d'une carte : %s", e)
        return None


def scrape(pages=3):
    """Scrape plusieurs pages d’annonces immobilières."""
    results = []
    for p in range(1, pages + 1):
        url = BASE_URL.format(page=p)
        logger.info("▶ Scraping page %d : %s", p, url)
        soup = get_soup(url)
        cards = soup.select("li.c-pa-list li.c-pa-item")
        if not cards:
            logger.warning("Aucune carte trouvée à la page %d, arrêt du scraping.", p)
            break
        for c in cards:
            data = parse_card(c)
            if data:
                results.append(data)
        sleep_time = random.uniform(1, 2)
        logger.debug(
            "Pause aléatoire de %.2f secondes avant la page suivante.", sleep_time
        )
        time.sleep(sleep_time)
    logger.info("Scraping terminé : %d annonces collectées.", len(results))
    return results


def save_csv(data):
    """Sauvegarde les annonces dans un CSV daté."""
    out = Path("data") / f"seloger_listings_{datetime.today():%Y%m%d}.csv"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=data[0].keys())
        w.writeheader()
        w.writerows(data)
    logger.info("✅ %d annonces sauvegardées dans %s", len(data), out)


if __name__ == "__main__":
    logger.info("Démarrage du scraping Seloger...")
    ads = scrape(pages=2)
    if ads:
        save_csv(ads)
    else:
        logger.warning("⚠️ Aucun résultat récupéré.")
