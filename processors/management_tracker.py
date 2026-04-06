import json
import logging

log = logging.getLogger(__name__)

MANAGEMENT_PROMPT = """
Analizujesz wzmiankę medialną dotyczącą osoby z zarządu Pekao TFI.

OSOBA: {name} ({role})

TYTUŁ: {title}
TREŚĆ: {content}

Zwróć JSON w tym formacie:
{{
  "dotyczy_osoby": true/false,
  "typ_wypowiedzi": "strategiczna|rynkowa|kryzysowa|PR|brak_wypowiedzi",
  "sentyment": 1-10,
  "cytat_kluczowy": "najważniejsze zdanie lub null",
  "temat": "wyniki funduszy|zarządzanie|regulacje|ESG|inne",
  "pilnosc": "wysoka|srednia|niska",
  "podsumowanie": "1-2 zdania po polsku"
}}

Zawsze odpowiadaj TYLKO w formacie JSON.
"""

class ManagementTracker:
    def __init__(self, keywords: dict, gemini_engine):
        self.gemini = gemini_engine
        self.board = keywords.get("management", {}).get("board", [])

    def check(self, articles: list[dict]) -> list[dict]:
        mentions = []

        for article in articles:
            text = (
                article.get("title", "") + " " +
                article.get("content", "")
            ).lower()

            for person in self.board:
                # Sprawdź czy artykuł wspomina tę osobę
                is_mentioned = any(
                    kw.lower() in text
                    for kw in person.get("keywords", [])
                )

                if is_mentioned:
                    log.info(f"Wzmianka o {person['name']} w: {article.get('title')}")
                    result = self._analyze(article, person)
                    if result.get("dotyczy_osoby"):
                        mentions.append({
                            "person": person["name"],
                            "role": person["role"],
                            "article_url": article.get("url"),
                            "article_title": article.get("title"),
                            "source": article.get("source"),
                            "date": article.get("date"),
                            **result
                        })

        log.info(f"Znaleziono {len(mentions)} wzmianek o zarządzie")
        return mentions

    def _analyze(self, article: dict, person: dict) -> dict:
        try:
            prompt = MANAGEMENT_PROMPT.format(
                name=person["name"],
                role=person["role"],
                title=article.get("title", ""),
                content=article.get("content", "")[:2000]
            )

            response = self.gemini.model.generate_content(prompt)
            return json.loads(response.text)

        except Exception as e:
            log.error(f"Błąd analizy zarządu {person['name']}: {e}")
            return {"dotyczy_osoby": False}
