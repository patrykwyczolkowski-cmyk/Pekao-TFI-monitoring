#!/usr/bin/env python3
"""
Pekao TFI Media Monitor — Daily Runner
Uruchamiany przez GitHub Actions codziennie.
"""

import time
import yaml
import logging
from datetime import datetime

from scrapers.rss_scraper import RssScraper
from scrapers.bankier_scraper import BankierScraper
from scrapers.wykop_scraper import WykopScraper
from scrapers.youtube_scraper import YouTubeScraper
from scrapers.reddit_scraper import RedditScraper
from scrapers.competitor_scraper import CompetitorScraper
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
    rpm_limit = config.get("limits", {}).get("gemini_requests_per_minute", 15)
    delay = 60.0 / rpm_limit

    # 1. Zbierz wzmianki Pekao TFI
    log.info("--- Krok 1: Zbieranie wzmianek Pekao TFI ---")
    articles = []
    articles += RssScraper(keywords).fetch()
    articles += BankierScraper(keywords).fetch()
    articles += WykopScraper(keywords).fetch()
    articles += YouTubeScraper(keywords).fetch()
    articles += RedditScraper(keywords).fetch()
    log.info(f"Zebrano łącznie: {len(articles)} artykułów")

    # 2. Deduplikacja
    log.info("--- Krok 2: Deduplikacja ---")
    sheets = SheetsClient(config)
    dedup = Deduplicator(sheets)
    articles = dedup.filter_new(articles)
    log.info(f"Nowych wzmianek do analizy: {len(articles)}")

    # 3. Analiza Gemini — Pekao TFI
    gemini = GeminiEngine(config)

    if articles:
        log.info("--- Krok 3: Analiza AI (Gemini) — Pekao TFI ---")
        results = []
        for i, article in enumerate(articles):
            log.info(f"Analizuję [{i+1}/{len(articles)}]: {article.get('title', '')[:50]}")
            result = gemini.analyze(article)
            results.append(result)
            if i < len(articles) - 1:
                time.sleep(delay)

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
    else:
        log.info("Brak nowych wzmianek Pekao TFI")

    # 7. Monitoring konkurencji (niezależny od wzmianek Pekao TFI)
    log.info("--- Krok 7: Monitoring konkurencji ---")
    comp_scraper = CompetitorScraper(keywords, gemini, rpm_limit=rpm_limit)
    comp_scraper.load_url_cache(sheets.get_competitor_url_cache())
    comp_mentions = comp_scraper.fetch_and_analyze()

    if comp_mentions:
        sheets.append_competitors(comp_mentions)
        new_hashes = [m["url_hash"] for m in comp_mentions if m.get("url_hash")]
        sheets.append_competitor_url_cache(new_hashes)
        log.info(f"Zapisano {len(comp_mentions)} wzmianek o konkurencji")
    else:
        log.info("Brak nowych wzmianek o konkurencji")

    log.info("=== KONIEC: Pekao TFI Monitor ===")

if __name__ == "__main__":
    main()
