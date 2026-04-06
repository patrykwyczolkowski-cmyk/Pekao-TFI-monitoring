import io
import os
import json
import logging
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.units import cm
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import gspread

log = logging.getLogger(__name__)

# Kolory Pekao
PEKAO_RED = HexColor("#CC0000")
PEKAO_DARK = HexColor("#1A1A2E")
PEKAO_GRAY = HexColor("#F5F5F5")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Nagłówki kolumn w Google Sheets (raw_data)
RAW_DATA_HEADERS = [
    "date", "source", "title", "url",
    "sentyment_artykul", "sentyment_komentarze", "sentyment_koncowy",
    "kategoria", "podsumowanie", "pilnosc", "wymaga_reakcji", "liczba_komentarzy"
]


class ReportGenerator:
    def __init__(self, config: dict):
        self.config = config
        self.drive_config = config.get("drive", {})
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        creds_dict = json.loads(creds_json)
        self.creds = Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES
        )
        self.drive_service = build("drive", "v3", credentials=self.creds)
        gc = gspread.authorize(self.creds)
        spreadsheet_id = os.environ.get("SPREADSHEET_ID")
        self.spreadsheet = gc.open_by_key(spreadsheet_id)

    def generate_weekly(self):
        log.info("Generuję raport tygodniowy...")
        data = self._get_data(days=7)
        if not data:
            log.info("Brak danych za ostatni tydzień")
            return

        week = datetime.now().strftime("%Y-W%V")
        base_name = f"raport_{week}"

        self._save_csv(data, base_name, "weekly")
        self._save_json(data, base_name, "weekly")
        self._save_pdf(data, base_name, "weekly", "Raport Tygodniowy")
        log.info(f"Raport tygodniowy {week} wygenerowany")

    def generate_monthly(self):
        log.info("Generuję raport miesięczny...")
        data = self._get_data(days=30)
        if not data:
            log.info("Brak danych za ostatni miesiąc")
            return

        month = datetime.now().strftime("%Y-%m")
        base_name = f"raport_{month}"

        self._save_csv(data, base_name, "monthly")
        self._save_json(data, base_name, "monthly")
        self._save_pdf(data, base_name, "monthly", "Raport Miesięczny")
        log.info(f"Raport miesięczny {month} wygenerowany")

    def _get_data(self, days: int) -> list[dict]:
        try:
            sheet = self.spreadsheet.worksheet(
                self.config["sheets"]["tabs"]["raw_data"]
            )
            records = sheet.get_all_records()
            cutoff = datetime.now() - timedelta(days=days)
            filtered = []
            for r in records:
                raw_date = r.get("date", "")
                if not raw_date:
                    continue
                try:
                    date = datetime.fromisoformat(str(raw_date))
                    if date >= cutoff:
                        filtered.append(r)
                except ValueError:
                    log.warning(f"Niepoprawna data w rekordzie: {raw_date!r} — pomijam")
            return filtered
        except Exception as e:
            log.error(f"Błąd pobierania danych z Sheets: {e}")
            return []

    def _save_csv(self, data: list[dict], name: str, folder_key: str):
        try:
            df = pd.DataFrame(data)
            path = f"/tmp/{name}.csv"
            df.to_csv(path, index=False, encoding="utf-8-sig")
            self._upload_to_drive(path, f"{name}.csv", folder_key)
            log.info(f"CSV zapisany: {name}.csv")
        except Exception as e:
            log.error(f"Błąd generowania CSV: {e}")

    def _save_json(self, data: list[dict], name: str, folder_key: str):
        try:
            summary = self._calculate_summary(data)
            output = {
                "generated_at": datetime.now().isoformat(),
                "summary": summary,
                "mentions": data
            }
            path = f"/tmp/{name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            self._upload_to_drive(path, f"{name}.json", folder_key)
            log.info(f"JSON zapisany: {name}.json")
        except Exception as e:
            log.error(f"Błąd generowania JSON: {e}")

    def _save_pdf(self, data: list[dict], name: str, folder_key: str, title: str):
        try:
            path = f"/tmp/{name}.pdf"
            summary = self._calculate_summary(data)
            doc = SimpleDocTemplate(
                path,
                pagesize=A4,
                rightMargin=2*cm,
                leftMargin=2*cm,
                topMargin=2*cm,
                bottomMargin=2*cm
            )
            styles = getSampleStyleSheet()
            story = []

            # Nagłówek
            title_style = ParagraphStyle(
                "Title",
                fontSize=24,
                textColor=PEKAO_RED,
                spaceAfter=0.5*cm,
                fontName="Helvetica-Bold"
            )
            story.append(Paragraph(f"Pekao TFI — {title}", title_style))
            story.append(Paragraph(
                f"Wygenerowano: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                styles["Normal"]
            ))
            story.append(Spacer(1, 0.5*cm))

            # Podsumowanie
            section_style = ParagraphStyle(
                "Section",
                fontSize=12,
                textColor=PEKAO_DARK,
                fontName="Helvetica-Bold",
                spaceAfter=0.3*cm
            )
            story.append(Paragraph("Podsumowanie", section_style))

            summary_data = [
                ["Wskaźnik", "Wartość"],
                ["Liczba wzmianek", str(summary["total_mentions"])],
                ["Średni sentyment", f"{summary['avg_score']:.1f}/10"],
                ["Alerty kryzysowe", str(summary["crisis_count"])],
                ["Pochwały", str(summary["praise_count"])],
                ["Najczęstsze źródło", summary["top_source"]],
            ]

            table = Table(summary_data, colWidths=[8*cm, 8*cm])
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), PEKAO_RED),
                ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#FFFFFF")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 11),
                ("BACKGROUND", (0, 1), (-1, -1), PEKAO_GRAY),
                ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#CCCCCC")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1),
                 [HexColor("#FFFFFF"), PEKAO_GRAY]),
                ("PADDING", (0, 0), (-1, -1), 8),
            ]))
            story.append(table)
            story.append(Spacer(1, 0.5*cm))

            # Wykres trendu sentymentu
            chart_image = self._build_trend_chart(data)
            if chart_image:
                story.append(Paragraph("Trend sentymentu", section_style))
                story.append(chart_image)
                story.append(Spacer(1, 0.5*cm))

            # Wzmianki
            story.append(Paragraph("Wzmianki (top 50)", section_style))

            item_style = ParagraphStyle(
                "Item",
                fontSize=10,
                spaceAfter=0.15*cm
            )
            desc_style = ParagraphStyle(
                "Desc",
                fontSize=9,
                textColor=HexColor("#555555"),
                spaceAfter=0.25*cm
            )

            for item in data[:50]:
                score = item.get("sentyment_koncowy", 5)
                try:
                    score = float(score)
                except (TypeError, ValueError):
                    score = 5.0

                color = (
                    "#CC0000" if score <= 3
                    else "#228B22" if score >= 9
                    else "#1A1A2E"
                )
                styled_item = ParagraphStyle(
                    "ItemDynamic",
                    parent=item_style,
                    textColor=HexColor(color)
                )
                story.append(Paragraph(
                    f"[{score:.0f}/10] {item.get('title', '')} "
                    f"— {item.get('source', '')} "
                    f"({item.get('date', '')[:10]})",
                    styled_item
                ))
                if item.get("podsumowanie"):
                    story.append(Paragraph(item["podsumowanie"], desc_style))

            doc.build(story)
            self._upload_to_drive(path, f"{name}.pdf", folder_key)
            log.info(f"PDF zapisany: {name}.pdf")

        except Exception as e:
            log.error(f"Błąd generowania PDF: {e}")

    def _build_trend_chart(self, data: list[dict]):
        """Generuje wykres liniowy trendu sentymentu i zwraca reportlab Image."""
        try:
            rows = []
            for r in data:
                raw_date = r.get("date", "")
                score = r.get("sentyment_koncowy")
                if not raw_date or score is None:
                    continue
                try:
                    date = datetime.fromisoformat(str(raw_date))
                    score = float(score)
                    rows.append({"date": date, "score": score})
                except (ValueError, TypeError):
                    continue

            if not rows:
                return None

            df = pd.DataFrame(rows).sort_values("date")
            # Grupuj po dniu, średnia sentymentu
            df["day"] = df["date"].dt.date
            daily = df.groupby("day")["score"].mean().reset_index()

            fig, ax = plt.subplots(figsize=(14, 4))
            ax.plot(
                [datetime.combine(d, datetime.min.time()) for d in daily["day"]],
                daily["score"],
                color="#CC0000",
                linewidth=2,
                marker="o",
                markersize=4
            )
            ax.axhline(y=3, color="#FF6666", linestyle="--", linewidth=1, label="Próg kryzysu (3)")
            ax.axhline(y=9, color="#66BB66", linestyle="--", linewidth=1, label="Próg pochwały (9)")
            ax.set_ylim(1, 10)
            ax.set_ylabel("Sentyment (1–10)")
            ax.set_xlabel("Data")
            ax.legend(fontsize=8)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
            ax.xaxis.set_major_locator(mdates.DayLocator())
            plt.xticks(rotation=45)
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)

            return Image(buf, width=16*cm, height=5*cm)

        except Exception as e:
            log.error(f"Błąd generowania wykresu: {e}")
            return None

    def _calculate_summary(self, data: list[dict]) -> dict:
        if not data:
            return {
                "total_mentions": 0,
                "avg_score": 0,
                "crisis_count": 0,
                "praise_count": 0,
                "top_source": "brak"
            }
        scores = []
        for r in data:
            val = r.get("sentyment_koncowy")
            if val is not None:
                try:
                    scores.append(float(val))
                except (TypeError, ValueError):
                    pass

        sources = [r.get("source", "") for r in data]
        top_source = max(set(sources), key=sources.count) if sources else "brak"

        return {
            "total_mentions": len(data),
            "avg_score": round(sum(scores) / len(scores), 2) if scores else 0,
            "crisis_count": sum(1 for s in scores if s <= 3),
            "praise_count": sum(1 for s in scores if s >= 9),
            "top_source": top_source
        }

    def _get_or_create_folder(self, folder_name: str, parent_id: str = None) -> str:
        query = (
            f"name='{folder_name}' and mimeType="
            f"'application/vnd.google-apps.folder' and trashed=false"
        )
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.drive_service.files().list(
            q=query, fields="files(id, name)"
        ).execute()
        files = results.get("files", [])

        if files:
            return files[0]["id"]

        metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder"
        }
        if parent_id:
            metadata["parents"] = [parent_id]

        folder = self.drive_service.files().create(
            body=metadata, fields="id"
        ).execute()
        return folder["id"]

    def _upload_to_drive(self, local_path: str, filename: str, folder_key: str):
        try:
            root = self._get_or_create_folder(
                self.drive_config.get("root_folder", "Pekao TFI — Monitoring")
            )
            subfolder_name = self.drive_config.get(
                "subfolders", {}
            ).get(folder_key, folder_key)
            folder_id = self._get_or_create_folder(subfolder_name, root)

            mime_types = {
                ".pdf": "application/pdf",
                ".csv": "text/csv",
                ".json": "application/json"
            }
            ext = "." + filename.split(".")[-1]
            mime_type = mime_types.get(ext, "application/octet-stream")

            metadata = {"name": filename, "parents": [folder_id]}
            media = MediaFileUpload(local_path, mimetype=mime_type)
            self.drive_service.files().create(
                body=metadata,
                media_body=media,
                fields="id"
            ).execute()
            log.info(f"Przesłano na Drive: {filename}")

        except Exception as e:
            log.error(f"Błąd uploadu na Drive {filename}: {e}")
