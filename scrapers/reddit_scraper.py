import requests
import hashlib
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

REDDIT_API = "https://www.reddit.com/search.json"
SUBREDDITS = [
    "r/Polska",
    "r/PolishInvestor",
    "r/giełda",
    "r/inwestycje",
]
HEADERS = {
    "User-Agent": "PekaoTFI-MediaMonitor/1.0 (media monitoring bot)"
}


class RedditScraper:
    def __init__(self, keywords: dict):
        self.keywords = keywords
        self.queries = [
            "Pekao TFI",
            "Pekao Towarzystwo Funduszy",
            "fundusze Pekao",
        ]

    def fetch(self) -> list[dict]:
        results = []
        for query in self.queries:
            results += self._search_global(query)
        # Deduplicate by URL within this run
        seen = set()
        unique = []
        for r in results:
            if r["url"] not in seen:
                seen.add(r["url"])
                unique.append(r)
        log.info(f"Reddit: znaleziono {len(unique)} unikalnych wzmianek")
        return unique

    def _search_global(self, query: str) -> list[dict]:
        try:
            resp = requests.get(
                REDDIT_API,
                params={
                    "q": query,
                    "sort": "new",
                    "t": "week",
                    "type": "link",
                    "limit": 25,
                    "restrict_sr": False,
                },
                headers=HEADERS,
                timeout=10
            )
            if resp.status_code != 200:
                log.error(f"Reddit API: błąd HTTP {resp.status_code} dla '{query}'")
                return []

            data = resp.json()
            posts = data.get("data", {}).get("children", [])
            results = []

            for post in posts:
                pd = post.get("data", {})
                url = f"https://www.reddit.com{pd.get('permalink', '')}"
                title = pd.get("title", "")
                selftext = pd.get("selftext", "")
                subreddit = pd.get("subreddit_name_prefixed", "")
                created_utc = pd.get("created_utc")

                if created_utc:
                    date = datetime.fromtimestamp(
                        created_utc, tz=timezone.utc
                    ).isoformat()
                else:
                    date = datetime.now(tz=timezone.utc).isoformat()

                # Pobierz komentarze z top-level JSON
                comments = self._fetch_comments(pd.get("id", ""), pd.get("subreddit", ""))

                content = selftext if selftext else title
                results.append({
                    "url": url,
                    "url_hash": hashlib.md5(url.encode()).hexdigest(),
                    "title": title,
                    "content": content,
                    "source": f"Reddit/{subreddit}",
                    "type": "social",
                    "date": date,
                    "comments": comments
                })

            return results

        except Exception as e:
            log.error(f"Błąd Reddit search '{query}': {e}")
            return []

    def _fetch_comments(self, post_id: str, subreddit: str) -> list[str]:
        if not post_id or not subreddit:
            return []
        try:
            url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                return []

            data = resp.json()
            if len(data) < 2:
                return []

            comments = []
            for item in data[1].get("data", {}).get("children", [])[:10]:
                body = item.get("data", {}).get("body", "")
                if body and body != "[deleted]" and body != "[removed]":
                    comments.append(body)
            return comments

        except Exception as e:
            log.error(f"Błąd pobierania komentarzy Reddit {post_id}: {e}")
            return []
