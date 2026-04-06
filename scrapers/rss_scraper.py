import feedparser
import hashlib
import logging
from datetime import datetime
from dateutil import parser as dateparser

log = logging.getLogger(__name__)

class RssScraper:
    def __init__(self, keywords: dict):
        self.keywords = keywords
        self.sources = keywords.get("sources", {}).get("rss", [])

    def fetch(self) -> list[dict]:
        articles = []
        for source in self.sources:
            log.info(f"Scrapuję RSS: {source['name']}")
            try:
                feed = feedparser.parse(source["url"])
                for entry in feed.entries:
                    article = self._parse_entry(entry, source["name"])
                    if article and self._is_relevant(article):
                        articles.append(article)
            except Exception as e:
                log.error(f"Błąd RSS {source['name']}: {e}")
        return articles

    def _parse_entry(self, entry, source_name: str) -> dict | None:
        try:
            url = entry.get("link", "")
            title = entry.get("title", "")
            summary = entry.get("summary", "")
            pub_date = entry.get("published", "")

            try:
                date = dateparser.parse(pub_date).isoformat()
            except:
                date = datetime.now().isoformat()

            return {
                "url": url,
                "url_hash": hashlib.md5(url.encode()).hexdigest(),
                "title": title,
                "content": summary,
                "source": source_name,
                "type": "news",
                "date": date,
                "comments": []
            }
        except Exception as e:
            log.error(f"Błąd parsowania wpisu: {e}")
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
