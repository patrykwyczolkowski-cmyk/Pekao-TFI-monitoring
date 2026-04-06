#!/usr/bin/env python3
"""
Pekao TFI Media Monitor — Daily Runner
Uruchamiany przez GitHub Actions codziennie.
"""

import yaml
import logging
from datetime import datetime

from scrapers.rss_scraper import RssScraper
from scrapers.bankier_scraper import BankierScraper
from scrapers.wykop_scraper import WykopScraper 
from scrapers.youtube_scraper import YouTubeScraper
from processors.deduplicator import Deduplicator
from processors.gemini_engine import GeminiEngine
from processors.management_tracker import ManagementTracker
from storage.sheets_client import SheetsClient
from alerts.alert_engine import AlertEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

def load_config():
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)
    with open("config/keywords.yaml") as f:
        keywords = yaml.safe_load(f)
    return config, keywords

def main():
    log.info("=== START: Pekao TFI Monitor ===")
    log.info(f"Czas uruchomienia: {datetime.now().isoformat()}")

    config, keywords = load_config()

    # 1. Zbierz wzmianki
    log.info("--- Krok 1: Zbieranie wzmianek ---")
    articles = []
    articles += RssScraper(keywords).fetch()
    articles += BankierScraper(keywords).fetch()
    articles += WykopScraper(keywords).fetch() 
    articles += YouTubeScraper(keywords).fetch()
    log.info(f"Zebrano łącznie: {len(articles)} artykułów")

    # 2. Deduplikacja
    log.info("--- Krok 2: Deduplikacja ---")
    sheets = SheetsClient(config)
    dedup = Deduplicator(sheets)
    articles = dedup.filter_new(articles)
    log.info(f"Nowych wzmianek do analizy: {len(articles)}")

    if not articles:
        log.info("Brak nowych wzmianek — kończę")
        return

    # 3. Analiza Gemini
    log.info("--- Krok 3: Analiza AI (Gemini) ---")
    gemini = GeminiEngine(config)
    results = []
    for i, article in enumerate(articles):
        log.info(f"Analizuję [{i+1}/{len(articles)}]: {article.get('title', '')[:50]}")
        result = gemini.analyze(article)
        results.append(result)

    # 4. Monitoring zarządu
    log.info("--- Krok 4: Monitoring zarządu ---")
    mgmt = ManagementTracker(keywords, gemini)
    mgmt_mentions = mgmt.check(articles)

    # 5. Zapis do Sheets
    log.info("--- Krok 5: Zapis do Google Sheets ---")
    sheets.append_results(results)
    sheets.append_management(mgmt_mentions)

    # 6. Sprawdź alerty
    log.info("--- Krok 6: Sprawdzanie alertów ---")
    alert_engine = AlertEngine(config, results)
    alert_engine.check_and_send()

    log.info("=== KONIEC: Pekao TFI Monitor ===")

if __name__ == "__main__":
    main()
