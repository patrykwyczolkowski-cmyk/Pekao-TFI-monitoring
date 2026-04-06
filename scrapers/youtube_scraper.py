import os
import requests
import hashlib
import logging
from datetime import datetime

log = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

class YouTubeScraper:
    def __init__(self, keywords: dict):
        self.api_key = os.environ.get("YOUTUBE_API_KEY")
        self.keywords = keywords
        self.queries = [
            "Pekao TFI",
            "Pekao Towarzystwo Funduszy Inwestycyjnych",
            "fundusze Pekao"
        ]

    def fetch(self) -> list[dict]:
        if not self.api_key:
            log.warning("Brak YOUTUBE_API_KEY — pomijam YouTube")
            return []

        results = []
        for query in self.queries:
            results += self._search_videos(query)
        return results

    def _search_videos(self, query: str) -> list[dict]:
        try:
            url = f"{YOUTUBE_API_BASE}/search"
            params = {
                "part": "snippet",
                "q": query,
                "type": "video",
                "maxResults": 10,
                "order": "date",
                "relevanceLanguage": "pl",
                "key": self.api_key
            }
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()

            results = []
            for item in data.get("items", []):
                video_id = item["id"]["videoId"]
                snippet = item["snippet"]
                video_url = f"https://youtube.com/watch?v={video_id}"

                comments = self._fetch_comments(video_id)

                results.append({
                    "url": video_url,
                    "url_hash": hashlib.md5(video_url.encode()).hexdigest(),
                    "title": snippet.get("title", ""),
                    "content": snippet.get("description", ""),
                    "source": "YouTube",
                    "type": "video",
                    "channel": snippet.get("channelTitle", ""),
                    "date": snippet.get("publishedAt", datetime.now().isoformat()),
                    "comments": comments
                })

            log.info(f"YouTube: znaleziono {len(results)} filmów dla '{query}'")
            return results

        except Exception as e:
            log.error(f"Błąd YouTube search: {e}")
            return []

    def _fetch_comments(self, video_id: str) -> list[str]:
        try:
            url = f"{YOUTUBE_API_BASE}/commentThreads"
            params = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": 20,
                "order": "relevance",
                "key": self.api_key
            }
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()

            comments = []
            for item in data.get("items", []):
                text = (
                    item["snippet"]["topLevelComment"]
                    ["snippet"]["textDisplay"]
                )
                if text:
                    comments.append(text)
            return comments

        except Exception as e:
            log.error(f"Błąd pobierania komentarzy YouTube {video_id}: {e}")
            return []
