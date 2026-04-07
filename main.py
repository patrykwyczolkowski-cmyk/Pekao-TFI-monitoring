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
from alerts.email_notifier import EmailNotifier

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
    run_start = datetime.now()
    log.info("=== START: Pekao TFI Monitor ===")
    log.info(f"Czas uruchomienia: {run_start.isoformat()}")

    config, keywords = load_config()
    rpm_limit = config.get("limits", {}).get("gemini_requests_per_minute", 15)
    delay = 60.0 / rpm_limit

    # Statystyki zbierane przez cały run — trafią do daily digest
    stats = {
        "date": run_start.strftime("%d.%m.%Y"),
        "run_time": run_start.strftime("%H:%M"),
        "pekao_scraped": 0,
        "pekao_new": 0,
        "pekao_analyzed": 0,
        "pekao_avg_sentiment": None,
        "pekao_crisis": 0,
        "pekao_praise": 0,
        "pekao_mgmt_mentions": 0,
        "competitors": {},
        "alerts_sent": [],
        "blog_mentions": 0,
        "status": "OK",
        "errors": [],
    }

    notifier = EmailNotifier(config)

    try:
        # 1. Zbierz wzmianki Pekao TFI
        log.info("--- Krok 1: Zbieranie wzmianek Pekao TFI ---")
        articles = []
        articles += RssScraper(keywords).fetch()
        articles += BankierScraper(keywords).fetch()
        articles += WykopScraper(keywords).fetch()
        articles += YouTubeScraper(keywords).fetch()
        articles += RedditScraper(keywords).fetch()

        stats["pekao_scraped"] = len(articles)
        stats["blog_mentions"] = sum(1 for a in articles if a.get("type") == "blog")
        log.info(f"Zebrano łącznie: {len(articles)} artykułów")

        # 2. Deduplikacja
        log.info("--- Krok 2: Deduplikacja ---")
        sheets = SheetsClient(config)
        dedup = Deduplicator(sheets)
        articles = dedup.filter_new(articles)
        stats["pekao_new"] = len(articles)
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

            stats["pekao_analyzed"] = len(results)

            # Policz statystyki sentymentu
            pekao_results = [r for r in results if r.get("dotyczy_pekao_tfi")]
            scores = []
            for r in pekao_results:
                val = r.get("sentyment_koncowy")
                if val is not None:
                    try:
                        scores.append(float(val))
                    except (TypeError, ValueError):
                        pass
            if scores:
                stats["pekao_avg_sentiment"] = round(sum(scores) / len(scores), 1)
                stats["pekao_crisis"] = sum(1 for s in scores if s <= 3)
                stats["pekao_praise"] = sum(1 for s in scores if s >= 9)

            # 4. Monitoring zarządu
            log.info("--- Krok 4: Monitoring zarządu ---")
            mgmt = ManagementTracker(keywords, gemini)
            mgmt_mentions = mgmt.check(articles)
            stats["pekao_mgmt_mentions"] = len(mgmt_mentions)

            # 5. Zapis do Sheets
            log.info("--- Krok 5: Zapis do Google Sheets ---")
            sheets.append_results(results)
            sheets.append_management(mgmt_mentions)

            # 6. Sprawdź alerty
            log.info("--- Krok 6: Sprawdzanie alertów ---")
            alert_engine = AlertEngine(config, results)
            alert_engine.check_and_send()

            if stats["pekao_crisis"] > 0:
                stats["alerts_sent"].append(
                    f"KRYZYS: {stats['pekao_crisis']} wzmianek (sentyment <=3)"
                )
            if stats["pekao_praise"] > 0:
                stats["alerts_sent"].append(
                    f"POCHWALENIE: {stats['pekao_praise']} wzmianek (sentyment >=9)"
                )
        else:
            log.info("Brak nowych wzmianek Pekao TFI")

        # 7. Monitoring konkurencji
        log.info("--- Krok 7: Monitoring konkurencji ---")
        comp_scraper = CompetitorScraper(keywords, gemini, rpm_limit=rpm_limit)
        comp_scraper.load_url_cache(sheets.get_competitor_url_cache())
        comp_mentions = comp_scraper.fetch_and_analyze()

        # Zlicz per konkurent
        for m in comp_mentions:
            name = m.get("competitor", "Nieznany")
            stats["competitors"][name] = stats["competitors"].get(name, 0) + 1

        # Uzupełnij zerami konkurentów bez wzmianek
        for comp in keywords.get("competitors", []):
            name = comp["name"]
            stats["competitors"].setdefault(name, 0)

        if comp_mentions:
            sheets.append_competitors(comp_mentions)
            new_hashes = [m["url_hash"] for m in comp_mentions if m.get("url_hash")]
            sheets.append_competitor_url_cache(new_hashes)
            log.info(f"Zapisano {len(comp_mentions)} wzmianek o konkurencji")
        else:
            log.info("Brak nowych wzmianek o konkurencji")

    except Exception as e:
        log.error(f"Krytyczny błąd w pipeline: {e}", exc_info=True)
        stats["status"] = "BLAD"
        stats["errors"].append(str(e))

    finally:
        # Daily digest — wysyłany ZAWSZE, niezależnie od wyniku
        log.info("--- Wysyłam daily digest ---")
        notifier.send_daily_digest(stats)
        log.info("=== KONIEC: Pekao TFI Monitor ===")


if __name__ == "__main__":
    main()
