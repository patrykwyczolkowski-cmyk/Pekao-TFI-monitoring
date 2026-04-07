import yaml
import logging
import os
import time
from processors.gemini_engine import GeminiEngine
from storage.sheets_client import SheetsClient

# Konfiguracja logowania - wyniki będą widoczne w logach GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

def load_config():
    """Wczytuje konfigurację z pliku YAML."""
    config_path = "config/config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Nie znaleziono pliku {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    log.info("=== START: Naprawa błędnych wpisów w Google Sheets ===")
    config = load_config()
    
    # Inicjalizacja klientów (korzystają z Twoich sekretów w GitHubie)
    try:
        sheets = SheetsClient(config)
        gemini = GeminiEngine(config)
    except Exception as e:
        log.error(f"Błąd inicjalizacji: {e}")
        return
    
    # Pobranie limitów RPM (Requests Per Minute) z konfiguracji
    rpm_limit = config.get("limits", {}).get("gemini_requests_per_minute", 10)
    delay = 60.0 / rpm_limit

    # Lista zakładek do sprawdzenia (Pekao TFI oraz Konkurencja)
    tabs_to_fix = [
        (config["sheets"]["tabs"]["raw_data"], "pekao"),
        (config["sheets"]["tabs"].get("competitors", "competitors"), "competitor")
    ]

    for tab_name, mode in tabs_to_fix:
        repair_tab(sheets, gemini, tab_name, mode, delay)

    log.info("=== KONIEC: Proces naprawy zakończony ===")

def repair_tab(sheets, gemini, tab_name, mode, delay):
    """Przeszukuje arkusz i naprawia wiersze z błędami."""
    try:
        log.info(f"Sprawdzam zakładkę: {tab_name}")
        worksheet = sheets.spreadsheet.worksheet(tab_name)
        records = worksheet.get_all_records()
        headers = worksheet.row_values(1)
        
        # Wykrywamy błędy po tych słowach kluczowych w komórce 'podsumowanie'
        error_markers = ["Błąd analizy", "blad_analizy", "API key", "not found", "404", "400", "invalid"]
        
        for i, row in enumerate(records, start=2): # start=2 bo wiersz 1 to nagłówki
            summary = str(row.get("podsumowanie", ""))
            
            if any(marker in summary for marker in error_markers):
                title = row.get("title", "Brak tytułu")
                log.info(f"Naprawiam wiersz {i} w {tab_name}: {title[:40]}...")
                
                # Tworzymy uproszczony obiekt artykułu do ponownej analizy
                article_data = {
                    "title": title,
                    "source": row.get("source", "Nieznane"),
                    "content": title, # Używamy tytułu jako bazy jeśli oryginał nie jest dostępny
                    "url": row.get("url", ""),
                    "comments": [] 
                }
                
                try:
                    if mode == "competitor":
                        comp_name = row.get("competitor", "Nieznany")
                        res = gemini.analyze_competitor(article_data, comp_name)
                        
                        # Aktualizacja komórek (F, G, H w zakładce konkurencja)
                        worksheet.update_cell(i, headers.index("sentyment_koncowy") + 1, res["sentyment_koncowy"])
                        worksheet.update_cell(i, headers.index("kategoria") + 1, res["kategoria"])
                        worksheet.update_cell(i, headers.index("podsumowanie") + 1, res["podsumowanie"])
                    else:
                        res = gemini.analyze(article_data)
                        if res.get("dotyczy_pekao_tfi"):
                            # Aktualizacja komórek dla Pekao TFI
                            worksheet.update_cell(i, headers.index("sentyment_artykul") + 1, res["sentyment_artykul"])
                            worksheet.update_cell(i, headers.index("sentyment_komentarze") + 1, res["sentyment_komentarze"])
                            worksheet.update_cell(i, headers.index("sentyment_koncowy") + 1, res["sentyment_koncowy"])
                            worksheet.update_cell(i, headers.index("kategoria") + 1, res["kategoria"])
                            worksheet.update_cell(i, headers.index("podsumowanie") + 1, res["podsumowanie"])

                    log.info(f"  Sukces: Wiersz {i} naprawiony.")
                    time.sleep(delay) # Limit API
                except Exception as e:
                    log.error(f"  Błąd wiersza {i}: {e}")

    except Exception as e:
        log.error(f"Błąd dostępu do zakładki {tab_name}: {e}")

if __name__ == "__main__":
    main()
