import logging

log = logging.getLogger(__name__)

class Deduplicator:
    def __init__(self, sheets_client):
        self.sheets = sheets_client
        self.known_hashes = self._load_known_hashes()

    def _load_known_hashes(self) -> set:
        try:
            hashes = self.sheets.get_url_cache()
            log.info(f"Załadowano {len(hashes)} znanych URL-i z cache")
            return set(hashes)
        except:
            return set()

    def filter_new(self, articles: list[dict]) -> list[dict]:
        new_articles = []
        new_hashes = []

        for article in articles:
            h = article.get("url_hash")
            if h and h not in self.known_hashes:
                new_articles.append(article)
                new_hashes.append(h)
                self.known_hashes.add(h)

        if new_hashes:
            self.sheets.append_url_cache(new_hashes)

        log.info(f"Deduplikacja: {len(articles)} → {len(new_articles)} nowych")
        return new_articles
