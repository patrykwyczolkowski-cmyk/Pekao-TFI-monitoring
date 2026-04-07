import yaml
import logging
import os
import time
import google.generativeai as genai
from processors.gemini_engine import GeminiEngine
from storage.sheets_client import SheetsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

def load_config():
    with open("config/config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_diagnostics():
    log.info("--- START DIAGNOSTYKI ---")
    log.info(f"Wersja biblioteki google-generativeai: {genai.__version__}")
    
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.error("BRAK KLUCZA API!")
        return False

    genai.configure(api_key=api_key)
    
    try:
        models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        log.info(f"Dostępne modele dla tego klucza: {models}")
    except Exception as e:
        log.error(f"Błąd pobierania modeli: {e}")
    
    log.info("--- KONIEC DIAGNOSTYKI ---")
    return True

def main():
    if not run_diagnostics():
        return

    log.info("=== ROZPOCZĘCIE NAPRAWY ===")
    config = load_config()
    
    try:
        sheets = SheetsClient(config)
        gemini = GeminiEngine(config)
    except Exception as e:
        log.error(f"Błąd inicjalizacji: {e}")
        return

    delay = 60.0 / config.get("limits", {}).get("gemini_requests_per_minute", 10)
    
    tabs = [
        (config["sheets"]["tabs"]["raw_data"], "pekao"),
        (config["sheets"]["tabs"].get("competitors", "competitors"), "competitor")
    ]

    for tab_name, mode in tabs:
        try:
            log.info(f"Sprawdzam zakładkę: {tab_name}")
            worksheet = sheets.spreadsheet.worksheet(tab_name)
            records = worksheet.get_all_records()
            headers = worksheet.row_values(1)
            
            errors = ["404", "400", "Błąd analizy", "blad_analizy", "API key", "not found"]
            
            for i, row in enumerate(records, start=2):
                summary = str(row.get("podsumowanie", ""))
                
                if any(err in summary for err in errors):
                    title = row.get("title", "Brak tytułu")
                    log.info(f"Naprawiam wiersz {i}: {title[:40]}...")
                    
                    article_data = {
                        "title": title, "source": row.get("source", ""),
                        "content": title, "url": row.get("url", ""), "comments": []
                    }
                    
                    try:
                        if mode == "competitor":
                            comp_name = row.get("competitor", "Konkurent")
                            res = gemini.analyze_competitor(article_data, comp_name)
                            worksheet.update_cell(i, headers.index("sentyment_koncowy") + 1, res["sentyment_koncowy"])
                            worksheet.update_cell(i, headers.index("kategoria") + 1, res["kategoria"])
                            worksheet.update_cell(i, headers.index("podsumowanie") + 1, res["podsumowanie"])
                        else:
                            res = gemini.analyze(article_data)
                            if res.get("dotyczy_pekao_tfi"):
                                worksheet.update_cell(i, headers.index("sentyment_artykul") + 1, res["sentyment_artykul"])
                                worksheet.update_cell(i, headers.index("sentyment_komentarze") + 1, res["sentyment_komentarze"])
                                worksheet.update_cell(i, headers.index("sentyment_koncowy") + 1, res["sentyment_koncowy"])
                                worksheet.update_cell(i, headers.index("kategoria") + 1, res["kategoria"])
                                worksheet.update_cell(i, headers.index("podsumowanie") + 1, res["podsumowanie"])

                        log.info(f"  -> Wiersz {i} naprawiony.")
                        time.sleep(delay)
                    except Exception as e:
                        log.error(f"  -> BŁĄD wiersza {i}: {e}")
        except Exception as e:
            log.error(f"Błąd zakładki {tab_name}: {e}")

if __name__ == "__main__":
    main()
