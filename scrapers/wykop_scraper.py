import os
import requests
import hashlib
import logging
from datetime import datetime

log = logging.getLogger(__name__)

API_BASE = "https://wykop.pl/api/v3"


class WykopScraper:
    def __init__(self, keywords: dict):
        self.keywords = keywords
        api_key = os.environ.get("WYKOP_API_KEY")
        if api_key:
            self.headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            self.enabled = True
        else:
            log.warning("Brak WYKOP_API_KEY — pomijam Wykop")
            self.enabled = False

    def fetch(self) -> list[dict]:
        if not self.enabled:
            return []

        results = []
        results += self._search_entries("Pekao TFI")
        results += self._search_entries("pekaotfi")
        return results

    def _search_entries(self, query: str) -> list[dict]:
        try:
            url = f"{API_BASE}/search/entries"
            resp = requests.get(
                url,
                params={"q": query, "limit": 20},
                headers=self.headers,
                timeout=10
            )
            if resp.status_code == 401:
                log.error("Wykop API: błąd autoryzacji (401) — sprawdź WYKOP_API_KEY")
                return []
            if resp.status_code != 200:
                log.error(f"Wykop API: błąd HTTP {resp.status_code}")
                return []

            data = resp.json()
            entries = data.get("data", [])

            results = []
            for entry in entries:
                content = entry.get("content", "")
                entry_url = f"https://wykop.pl/wpis/{entry.get('id')}"
                comments = [
                    c.get("content", "")
                    for c in entry.get("comments", {}).get("data", [])
                    if c.get("content")
                ]

                results.append({
                    "url": entry_url,
                    "url_hash": hashlib.md5(entry_url.encode()).hexdigest(),
                    "title": content[:100] + "..." if len(content) > 100 else content,
                    "content": content,
                    "source": "Wykop",
                    "type": "social",
                    "date": entry.get("created_at", datetime.now().isoformat()),
                    "comments": comments
                })
            return results

        except Exception as e:
            log.error(f"Błąd Wykop API: {e}")
            return []
