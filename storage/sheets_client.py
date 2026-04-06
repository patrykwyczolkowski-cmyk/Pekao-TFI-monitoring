import os
import json
import time
import logging
import gspread
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Nagłówki dla każdej zakładki — tworzone automatycznie przy pierwszym uruchomieniu
RAW_DATA_HEADERS = [
    "date", "source", "title", "url",
    "sentyment_artykul", "sentyment_komentarze", "sentyment_koncowy",
    "kategoria", "podsumowanie", "pilnosc", "wymaga_reakcji", "liczba_komentarzy"
]
MANAGEMENT_HEADERS = [
    "date", "person", "role", "source", "article_title", "article_url",
    "typ_wypowiedzi", "sentyment", "temat", "cytat_kluczowy", "pilnosc", "podsumowanie"
]
URL_CACHE_HEADERS = ["url_hash"]


class SheetsClient:
    def __init__(self, config: dict):
        self.config = config
        self.spreadsheet_id = (
            os.environ.get("SPREADSHEET_ID")
            or config["sheets"]["spreadsheet_id"]
        )
        self.tabs = config["sheets"]["tabs"]
        self.client = self._authenticate()
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
        self._ensure_all_headers()

    def _authenticate(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise EnvironmentError(
                "Brak zmiennej środowiskowej GOOGLE_CREDENTIALS_JSON"
            )
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return gspread.authorize(creds)

    def _ensure_all_headers(self):
        """Automatycznie tworzy nagłówki we wszystkich zakładkach jeśli są puste."""
        self._ensure_headers(self.tabs["raw_data"], RAW_DATA_HEADERS)
        self._ensure_headers(self.tabs["management"], MANAGEMENT_HEADERS)
        self._ensure_headers(self.tabs["dedup_cache"], URL_CACHE_HEADERS)

    def _ensure_headers(self, tab_name: str, headers: list[str]):
        try:
            sheet = self._get_or_create_sheet(tab_name)
            first_row = sheet.row_values(1)
            if not first_row:
                sheet.append_row(headers)
                log.info(f"Dodano nagłówki do zakładki '{tab_name}'")
            elif first_row != headers:
                log.warning(
                    f"Nagłówki w '{tab_name}' różnią się od oczekiwanych — "
                    f"pomijam nadpisanie"
                )
        except Exception as e:
            log.error(f"Błąd sprawdzania nagłówków w '{tab_name}': {e}")

    def _get_or_create_sheet(self, tab_name: str):
        try:
            return self.spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            log.info(f"Zakładka '{tab_name}' nie istnieje — tworzę")
            return self.spreadsheet.add_worksheet(
                title=tab_name, rows=1000, cols=20
            )

    def _retry(self, func, *args, retries: int = 3, delay: float = 2.0, **kwargs):
        """Wykonuje funkcję z retry przy błędach sieciowych."""
        for attempt in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < retries - 1:
                    log.warning(
                        f"Błąd Sheets (próba {attempt+1}/{retries}): {e} — "
                        f"ponawiamy za {delay:.0f}s"
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    def append_results(self, results: list[dict]):
        """Zapisz wyniki analizy do zakładki raw_data."""
        try:
            sheet = self._get_or_create_sheet(self.tabs["raw_data"])
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
                    r.get("sentyment_koncowy", ""),
                    r.get("kategoria", ""),
                    r.get("podsumowanie", ""),
                    r.get("pilnosc", ""),
                    str(r.get("wymaga_reakcji", False)),
                    str(len(r.get("comments", [])))
                ])
            if rows:
                self._retry(sheet.append_rows, rows)
                log.info(f"Zapisano {len(rows)} wzmianek do Sheets")
        except Exception as e:
            log.error(f"Błąd zapisu do Sheets: {e}")

    def append_management(self, mentions: list[dict]):
        """Zapisz wzmianki o zarządzie."""
        try:
            sheet = self._get_or_create_sheet(self.tabs["management"])
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
                self._retry(sheet.append_rows, rows)
                log.info(f"Zapisano {len(rows)} wzmianek o zarządzie")
        except Exception as e:
            log.error(f"Błąd zapisu zarządu do Sheets: {e}")

    def get_url_cache(self) -> list[str]:
        """Pobierz znane hashe URL z cache."""
        try:
            sheet = self._get_or_create_sheet(self.tabs["dedup_cache"])
            values = sheet.col_values(1)
            # Pomiń wiersz nagłówkowy jeśli istnieje
            return [v for v in values if v and v != "url_hash"]
        except Exception as e:
            log.error(f"Błąd pobierania cache URL: {e}")
            return []

    def append_url_cache(self, hashes: list[str]):
        """Zapisz nowe hashe URL do cache."""
        try:
            sheet = self._get_or_create_sheet(self.tabs["dedup_cache"])
            rows = [[h] for h in hashes]
            self._retry(sheet.append_rows, rows)
        except Exception as e:
            log.error(f"Błąd zapisu cache: {e}")

    def get_recent_scores(self, days: int = 7) -> list[dict]:
        """Pobierz wyniki z ostatnich N dni."""
        try:
            from datetime import datetime, timedelta
            sheet = self._get_or_create_sheet(self.tabs["raw_data"])
            records = sheet.get_all_records()
            cutoff = datetime.now() - timedelta(days=days)
            filtered = []
            for r in records:
                raw_date = r.get("date", "")
                if not raw_date:
                    continue
                try:
                    if datetime.fromisoformat(str(raw_date)) >= cutoff:
                        filtered.append(r)
                except ValueError:
                    pass
            return filtered
        except Exception as e:
            log.error(f"Błąd pobierania wyników: {e}")
            return []
