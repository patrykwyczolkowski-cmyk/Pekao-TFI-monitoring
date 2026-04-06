import requests
import hashlib
import logging
from bs4 import BeautifulSoup
from datetime import datetime

log = logging.getLogger(__name__)

BASE_URL = "https://www.bankier.pl"
SEARCH_URL = f"{BASE_URL}/wiadomosci/lista/szukaj?query=Pekao+TFI"
FORUM_URL = f"{BASE_URL}/forum/szukaj?q=Pekao+TFI"

class BankierScraper:
    def __init__(self, keywords: dict):
        self.keywords = keywords
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            )
        }

    def fetch(self) -> list[dict]:
        articles = []
        articles += self._fetch_news()
        articles += self._fetch_forum()
        return articles

    def _fetch_news(self) -> list[dict]:
        results = []
        try:
            resp = requests.get(SEARCH_URL, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.select("a.article-link")[:10]

            for link in links:
                url = BASE_URL + link.get("href", "")
                title = link.get_text(strip=True)
                content, comments = self._fetch_article_with_comments(url)

                results.append({
                    "url": url,
                    "url_hash": hashlib.md5(url.encode()).hexdigest(),
                    "title": title,
                    "content": content,
                    "source": "Bankier",
                    "type": "news",
                    "date": datetime.now().isoformat(),
                    "comments": comments
                })
        except Exception as e:
            log.error(f"Błąd Bankier news: {e}")
        return results

    def _fetch_article_with_comments(self, url: str) -> tuple[str, list]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")

            content_div = soup.select_one("div.article-body")
            content = content_div.get_text(strip=True) if content_div else ""

            comments = []
            for c in soup.select("div.comment-content")[:20]:
                text = c.get_text(strip=True)
                if text and len(text) > 10:
                    comments.append(text)

            return content, comments
        except Exception as e:
            log.error(f"Błąd pobierania artykułu {url}: {e}")
            return "", []

    def _fetch_forum(self) -> list[dict]:
        results = []
        try:
            resp = requests.get(FORUM_URL, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            threads = soup.select("a.thread-title")[:10]

            for thread in threads:
                url = BASE_URL + thread.get("href", "")
                results.append({
                    "url": url,
                    "url_hash": hashlib.md5(url.encode()).hexdigest(),
                    "title": thread.get_text(strip=True),
                    "content": "",
                    "source": "Bankier Forum",
                    "type": "forum",
                    "date": datetime.now().isoformat(),
                    "comments": self._fetch_forum_posts(url)
                })
        except Exception as e:
            log.error(f"Błąd Bankier forum: {e}")
        return results

    def _fetch_forum_posts(self, url: str) -> list[str]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.text, "html.parser")
            return [
                p.get_text(strip=True)
                for p in soup.select("div.post-content")[:20]
                if len(p.get_text(strip=True)) > 10
            ]
        except:
            return []
