import feedparser
import hashlib
import logging
from datetime import datetime
from dateutil import parser as dateparser

log = logging.getLogger(__name__)


class RssScraper:
    def __init__(self, keywords: dict):
        self.keywords = keywords
        sources_cfg = keywords.get("sources", {})
        rss_sources = sources_cfg.get("rss", [])
        alert_sources = sources_cfg.get("google_alerts", [])
        blogger_sources = sources_cfg.get("bloggers", [])
        self.sources = rss_sources + alert_sources + blogger_sources

    def fetch(self) -> list[dict]:
        articles = []
        for source in self.sources:
            log.info(f"Scrapuję RSS: {source['name']}")
            try:
                feed = feedparser.parse(source["url"])
                if not feed.entries:
                    log.warning(f"Pusty feed lub brak wpisów: {source['name']}")
                    continue
                source_type = source.get("type", "news")
                for entry in feed.entries:
                    article = self._parse_entry(entry, source["name"], source_type)
                    if article and self._is_relevant(article):
                        articles.append(article)
            except Exception as e:
                log.error(f"Błąd RSS {source['name']}: {e}")
        return articles

    def _parse_entry(self, entry, source_name: str, source_type: str) -> dict | None:
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
                "type": source_type,
                "date": date,
                "comments": []
            }
        except Exception as e:
            log.error(f"Błąd parsowania wpisu RSS ({source_name}): {e}")
            return None

    def _is_relevant(self, article: dict) -> bool:
        text = (article["title"] + " " + article["content"]).lower()

        for excl in self.keywords.get("exclude", []):
            if excl.lower() in text:
                return False

        all_keywords = (
            self.keywords.get("primary", []) +
            self.keywords.get("secondary", []) +
            self.keywords.get("typos", [])
        )
        return any(kw.lower() in text for kw in all_keywords)
