import time
import hashlib
import logging
import feedparser
from datetime import datetime
from dateutil import parser as dateparser

log = logging.getLogger(__name__)


class CompetitorScraper:
    """
    Scrapuje wzmianki o konkurentach (PKO TFI, TFI PZU, Goldman Sachs TFI)
    i analizuje je przez Gemini. Dane są całkowicie oddzielone od danych Pekao TFI.
    """

    def __init__(self, keywords: dict, gemini_engine, rpm_limit: int = 15):
        self.competitors = keywords.get("competitors", [])
        self.gemini = gemini_engine
        self.delay = 60.0 / rpm_limit
        self._url_cache: set[str] = set()

    def load_url_cache(self, known_hashes: list[str]):
        """Wczytaj znane URL-e z Sheets żeby unikać duplikatów."""
        self._url_cache = set(known_hashes)

    def fetch_and_analyze(self) -> list[dict]:
        """
        Dla każdego konkurenta: pobierz artykuły z RSS, odfiltruj duplikaty,
        przeanalizuj Gemini. Zwraca listę wszystkich wzmianek z polem 'competitor'.
        """
        all_mentions = []

        for competitor in self.competitors:
            name = competitor["name"]
            log.info(f"Scrapuję konkurenta: {name}")
            articles = self._fetch_rss(competitor)
            log.info(f"  {name}: {len(articles)} nowych artykułów do analizy")

            for i, article in enumerate(articles):
                result = self.gemini.analyze_competitor(article, name)
                if result.get("dotyczy_konkurenta", True):
                    result["competitor"] = name
                    all_mentions.append(result)
                if i < len(articles) - 1:
                    time.sleep(self.delay)

        log.info(f"Konkurencja łącznie: {len(all_mentions)} wzmianek")
        return all_mentions

    def _fetch_rss(self, competitor: dict) -> list[dict]:
        """Pobierz i odfiltruj artykuły z RSS dla danego konkurenta."""
        name = competitor["name"]
        keywords = [kw.lower() for kw in competitor.get("keywords", [])]
        results = []

        for source in competitor.get("rss", []):
            try:
                feed = feedparser.parse(source["url"])
                for entry in feed.entries:
                    article = self._parse_entry(entry, source["name"], name)
                    if not article:
                        continue

                    # Deduplikacja
                    if article["url_hash"] in self._url_cache:
                        continue

                    # Filtrowanie tematyczne — artykuł musi dotyczyć konkurenta
                    text = (article["title"] + " " + article["content"]).lower()
                    if not any(kw in text for kw in keywords):
                        continue

                    self._url_cache.add(article["url_hash"])
                    results.append(article)

            except Exception as e:
                log.error(f"Błąd RSS {source['name']} ({name}): {e}")

        return results

    def _parse_entry(self, entry, source_name: str, competitor_name: str) -> dict | None:
        try:
            url = entry.get("link", "")
            if not url:
                return None

            title = entry.get("title", "")
            summary = entry.get("summary", "")
            pub_date = entry.get("published", "")

            try:
                date = dateparser.parse(pub_date).isoformat()
            except Exception:
                date = datetime.now().isoformat()

            return {
                "url": url,
                "url_hash": hashlib.md5(url.encode()).hexdigest(),
                "title": title,
                "content": summary,
                "source": source_name,
                "competitor": competitor_name,
                "type": "news",
                "date": date,
                "comments": [],
            }
        except Exception as e:
            log.error(f"Błąd parsowania wpisu konkurenta: {e}")
            return None
