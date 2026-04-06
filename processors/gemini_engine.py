import os
import json
import logging
import google.generativeai as genai

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """
Jestes analitykiem PR dla Pekao TFI (Pekao Towarzystwo Funduszy Inwestycyjnych).

KRYTYCZNE ROZROZNIENIA:
- "Pekao TFI" = Bank Pekao SA - podmiot monitorowany
- "PKO TFI" = PKO Bank Polski - KONKURENT, ignoruj
- "BPH TFI", "Pekao Pioneer" = historyczne nazwy Pekao TFI - monitoruj
- Literowki: "Pekao TIF", "Pekao TF1", "Pecao TFI" - monitoruj

SKALA OCEN SENTYMENTU (1-10):
1-3: Kryzys
4-6: Neutralne lub mieszane
7-8: Pozytywne
9-10: Wyjatkowe pochwaly

Zawsze odpowiadaj TYLKO w formacie JSON, bez zadnego tekstu przed ani po.
"""

ARTICLE_PROMPT = """Przeanalizuj wzmiankę medialną.

TYTUL: {title}
ZRODLO: {source}
TRESC: {content}

KOMENTARZE ({comment_count} szt.):
{comments}

Zwroc JSON:
{{"dotyczy_pekao_tfi": true, "sentyment_artykul": 5, "sentyment_komentarze": 5, "sentyment_koncowy": 5, "kategoria": "neutralna", "podsumowanie": "opis", "cytaty_kluczowe": [], "pilnosc": "niska", "wymaga_reakcji": false}}"""


class GeminiEngine:
    def __init__(self, config: dict):
        api_key = os.environ.get("GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=SYSTEM_PROMPT
        )
        self.config = config

    def analyze(self, article: dict) -> dict:
        try:
            comment_list = article.get("comments", [])
            if comment_list:
                comments_text = "".join(f"- {c}\n" for c in comment_list)
            else:
                comments_text = "Brak komentarzy"

            prompt = ARTICLE_PROMPT.format(
                title=article.get("title", ""),
                source=article.get("source", ""),
                content=article.get("content", "")[:2000],
                comment_count=len(comment_list),
                comments=comments_text[:1000]
            )

            response = self.model.generate_content(prompt)
            result = json.loads(response.text)
            return {**article, **result}

        except Exception as e:
            log.error(f"Blad Gemini: {e}")
            return {
                **article,
                "dotyczy_pekao_tfi": False,
                "sentyment_koncowy": 5,
                "kategoria": "blad_analizy",
                "podsumowanie": f"Blad: {str(e)}",
                "pilnosc": "niska",
                "wymaga_reakcji": False
            }
