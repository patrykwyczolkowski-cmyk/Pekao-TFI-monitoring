import os
import re
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

Zwroc JSON (wszystkie klucze bez polskich znakow):
{{"dotyczy_pekao_tfi": true, "sentyment_artykul": 5, "sentyment_komentarze": 5, "sentyment_koncowy": 5, "kategoria": "neutralna", "podsumowanie": "opis", "cytaty_kluczowe": [], "pilnosc": "niska", "wymaga_reakcji": false}}"""

COMPETITOR_PROMPT = """Przeanalizuj wzmiankę medialną dotyczącą firmy {competitor}.

TYTUL: {title}
ZRODLO: {source}
TRESC: {content}

Zwroc JSON (wszystkie klucze bez polskich znakow):
{{"dotyczy_konkurenta": true, "sentyment_koncowy": 5, "kategoria": "neutralna", "podsumowanie": "1-2 zdania po polsku"}}"""

REQUIRED_FIELDS = {
    "dotyczy_pekao_tfi": bool,
    "sentyment_artykul": (int, float),
    "sentyment_komentarze": (int, float),
    "sentyment_koncowy": (int, float),
    "kategoria": str,
    "podsumowanie": str,
    "pilnosc": str,
    "wymaga_reakcji": bool,
}


class GeminiEngine:
    def __init__(self, config: dict):
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise EnvironmentError("Brak zmiennej środowiskowej GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            system_instruction=SYSTEM_PROMPT
        )
        self.config = config

    def analyze(self, article: dict) -> dict:
        try:
            comment_list = article.get("comments", [])
            comments_text = (
                "".join(f"- {c}\n" for c in comment_list)
                if comment_list else "Brak komentarzy"
            )

            prompt = ARTICLE_PROMPT.format(
                title=article.get("title", "")[:500],
                source=article.get("source", ""),
                content=article.get("content", "")[:3000],
                comment_count=len(comment_list),
                comments=comments_text[:1500]
            )

            response = self.model.generate_content(prompt)
            result = self._parse_response(response.text)
            return {**article, **result}

        except Exception as e:
            log.error(f"Błąd Gemini dla '{article.get('title', '')[:50]}': {e}")
            return self._fallback(article, str(e))

    def _parse_response(self, text: str) -> dict:
        """Parsuje odpowiedź Gemini, wyodrębniając JSON nawet jeśli jest otoczony tekstem."""
        # Usuń ewentualne bloki markdown ```json ... ```
        cleaned = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()

        # Spróbuj znaleźć JSON w tekście
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            raise ValueError(f"Brak JSON w odpowiedzi Gemini: {text[:200]!r}")

        result = json.loads(match.group())
        self._validate(result)
        return result

    def _validate(self, result: dict):
        """Sprawdza czy wymagane pola istnieją i mają poprawny typ."""
        for field, expected_type in REQUIRED_FIELDS.items():
            if field not in result:
                log.warning(f"Brak pola '{field}' w odpowiedzi Gemini — ustawiam domyślną wartość")
                if expected_type == bool:
                    result[field] = False
                elif expected_type in ((int, float), int, float):
                    result[field] = 5
                else:
                    result[field] = ""
            elif not isinstance(result[field], expected_type):
                # Próbuj konwersję
                try:
                    if expected_type == bool:
                        result[field] = bool(result[field])
                    elif expected_type in ((int, float),):
                        result[field] = float(result[field])
                    else:
                        result[field] = str(result[field])
                except (TypeError, ValueError):
                    log.warning(f"Niepoprawny typ pola '{field}' — ustawiam domyślną wartość")
                    result[field] = False if expected_type == bool else 5 if expected_type in ((int, float),) else ""

        # Sprawdź zakres sentymentu
        for score_field in ("sentyment_artykul", "sentyment_komentarze", "sentyment_koncowy"):
            val = result.get(score_field, 5)
            try:
                result[score_field] = max(1, min(10, float(val)))
            except (TypeError, ValueError):
                result[score_field] = 5.0

    def analyze_competitor(self, article: dict, competitor_name: str) -> dict:
        """Uproszczona analiza dla artykułów o konkurencji."""
        try:
            prompt = COMPETITOR_PROMPT.format(
                competitor=competitor_name,
                title=article.get("title", "")[:500],
                source=article.get("source", ""),
                content=article.get("content", "")[:2000],
            )
            response = self.model.generate_content(prompt)
            result = self._parse_response(response.text)

            # Upewnij się że wymagane pola są obecne
            result.setdefault("dotyczy_konkurenta", True)
            result.setdefault("sentyment_koncowy", 5)
            result.setdefault("kategoria", "neutralna")
            result.setdefault("podsumowanie", "")
            try:
                result["sentyment_koncowy"] = max(1, min(10, float(result["sentyment_koncowy"])))
            except (TypeError, ValueError):
                result["sentyment_koncowy"] = 5.0

            return {**article, **result}

        except Exception as e:
            log.error(f"Błąd Gemini (konkurent {competitor_name}): {e}")
            return {
                **article,
                "dotyczy_konkurenta": True,
                "sentyment_koncowy": 5,
                "kategoria": "blad_analizy",
                "podsumowanie": f"Błąd analizy: {str(e)[:200]}",
            }

    def _fallback(self, article: dict, error_msg: str) -> dict:
        return {
            **article,
            "dotyczy_pekao_tfi": False,
            "sentyment_artykul": 5,
            "sentyment_komentarze": 5,
            "sentyment_koncowy": 5,
            "kategoria": "blad_analizy",
            "podsumowanie": f"Błąd analizy: {error_msg[:200]}",
            "cytaty_kluczowe": [],
            "pilnosc": "niska",
            "wymaga_reakcji": False,
        }
