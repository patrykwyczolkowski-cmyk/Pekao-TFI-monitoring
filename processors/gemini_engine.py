import os
import json
import logging
import google.generativeai as genai

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """
Jesteś analitykiem PR dla Pekao TFI (Pekao Towarzystwo Funduszy Inwestycyjnych).

KRYTYCZNE ROZRÓŻNIENIA:
- "Pekao TFI" = Bank Pekao SA → podmiot monitorowany ✅
- "PKO TFI" = PKO Bank Polski → KONKURENT, ignoruj ❌
- "BPH TFI", "Pekao Pioneer" = historyczne nazwy Pekao TFI → monitoruj ✅
- Literówki: "Pekao TIF", "Pekao TF1", "Pecao TFI" → monitoruj ✅

KONTEKST BIZNESOWY:
Pekao TFI zarządza funduszami inwestycyjnymi.
Wrażliwe tematy: opłaty za zarządzanie, wyniki funduszy,
zmiany w zarządzie, regulacje KNF, porównania z konkurencją.

SKALA OCEN SENTYMENTU (1-10):
1-3: Kryzys (błędy, straty, skandale, regulacje karne)
4-6: Neutralne lub mieszane
7-8: Pozytywne (dobre wyniki, nagrody, ekspansja)
9-10: Wyjątkowe pochwały, wyróżnienia branżowe

Zawsze odpowiadaj TYLKO w formacie JSON, bez żadnego tekstu przed ani po.
"""

ARTICLE_PROMPT = """
Przeanalizuj poniższą wzmiankę medialną.

TYTUŁ: {title}
ŹRÓDŁO: {source}
TREŚĆ: {content}

KOMENTARZE POD ARTYKUŁEM ({comment_count} szt.):
{comments}

Zwróć JSON w tym formacie:
{{
  "dotyczy_pekao_tfi": true/false,
  "sentyment_artykul": 1-10,
  "sentyment_komentarze": 1-10,
  "sentyment_końcowy": 1-10,
  "kategoria": "wyniki funduszy|zarządzanie|regulacje|ESG|kryzys|pochwała|neutralna",
  "podsumowanie": "2-3 zdania po polsku",
  "cytaty_kluczowe": ["cytat1", "cytat2"],
  "pilnosc": "wysoka|srednia|niska",
  "wymaga_reakcji": true/false
}}
"""

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
            comments_text = "\​​​​​​​​​​​​​​​​
