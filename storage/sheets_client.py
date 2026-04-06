import os
import json
import logging
import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

class SheetsClient:
    def __init__(self, config: dict):
        self.config = config
        self.spreadsheet_id = config["sheets"]["spreadsheet_id"]
        self.tabs = config["sheets"]["tabs"]
        self.client = self._authenticate()
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)

    def _authenticate(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES
        )
        return gspread.authorize(creds)

    def append_results(self, results: list[dict]):
        """Zapisz wyniki analizy do zakładki raw_data."""
        try:
            sheet = self.spreadsheet.worksheet(self.tabs["raw_data"])
            rows = []
            for r in results:
                if not r.get("dotyczy_pekao_tfi"):
                    continue
                rows.append([
                    r.get("date", ""),
                    r.get("source", ""),
                    r.get("title", ""),
                    r.get("url", ""),
                    r.get("sentyment_artykul", ""),
                    r.get("sentyment_komentarze", ""),
                    r.get("sentyment_końcowy", ""),
                    r.get("kategoria", ""),
                    r.get("podsumowanie", ""),
                    r.get("pilnosc", ""),
                    str(r.get("wymaga_reakcji", False)),
                    str(len(r.get("comments", [])))
                ])
            if rows:
                sheet.append_rows(rows)
                log.info(f"Zapisano {len(rows)} wzmianek do Sheets")
        except Exception as e:
            log.error(f"Błąd zapisu do Sheets: {e}")

    def append_management(self, mentions: list[dict]):
        """Zapisz wzmianki o zarządzie."""
        try:
            sheet = self.spreadsheet.worksheet(self.tabs["management"])
            rows = []
            for m in mentions:
                rows.append([
                    m.get("date", ""),
                    m.get("person", ""),
                    m.get("role", ""),
                    m.get("source", ""),
                    m.get("article_title", ""),
                    m.get("article_url", ""),
                    m.get("typ_wypowiedzi", ""),
                    m.get("sentyment", ""),
                    m.get("temat", ""),
                    m.get("cytat_kluczowy", ""),
                    m.get("pilnosc", ""),
                    m.get("podsumowanie", "")
                ])
            if rows:
                sheet.append_rows(rows)
                log.info(f"Zapisano {len(rows)} wzmianek o zarządzie")
        except Exception as e:
            log.error(f"Błąd zapisu zarządu do Sheets: {e}")

    def get_url_cache(self) -> list[str]:
        """Pobierz znane hashe URL z cache."""
        try:
            sheet = self.spreadsheet.worksheet(self.tabs["dedup_cache"])
            return sheet.col_values(1)
        except:
            return []

    def append_url_cache(self, hashes: list[str]):
        """Zapisz nowe hashe URL do cache."""
        try:
            sheet = self.spreadsheet.worksheet(self.tabs["dedup_cache"])
            rows = [[h] for h in hashes]
            sheet.append_rows(rows)
        except Exception as e:
            log.error(f"Błąd zapisu cache: {e}")

    def get_recent_scores(self, days: int = 7) -> list[dict]:
        """Pobierz wyniki z ostatnich N dni do analizy trendu."""
        try:
            sheet = self.spreadsheet.worksheet(self.tabs["raw_data"])
            records = sheet.get_all_records()
            return records[-days*20:]  # Przybliżenie
        except Exception as e:
            log.error(f"Błąd pobierania wyników: {e}")
            return []
