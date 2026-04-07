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
        comp_data = self._get_competitor_data(days=7)

        if not data and not comp_data:
            log.info("Brak danych za ostatni tydzień")
            return

        week = datetime.now().strftime("%Y-W%V")
        base_name = f"raport_{week}"

        self._save_csv(data, base_name, "weekly")
        self._save_json(data, comp_data, base_name, "weekly")
        self._save_pdf(data, comp_data, base_name, "weekly", "Raport Tygodniowy")
        log.info(f"Raport tygodniowy {week} wygenerowany")

    def generate_monthly(self):
        log.info("Generuję raport miesięczny...")
        data = self._get_data(days=30)
        comp_data = self._get_competitor_data(days=30)

        if not data and not comp_data:
            log.info("Brak danych za ostatni miesiąc")
            return

        month = datetime.now().strftime("%Y-%m")
        base_name = f"raport_{month}"

        self._save_csv(data, base_name, "monthly")
        self._save_json(data, comp_data, base_name, "monthly")
        self._save_pdf(data, comp_data, base_name, "monthly", "Raport Miesięczny")
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

    def _get_competitor_data(self, days: int) -> list[dict]:
        try:
            tab = self.config["sheets"]["tabs"].get("competitors", "competitors")
            sheet = self.spreadsheet.worksheet(tab)
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
                    log.warning(f"Niepoprawna data (konkurencja): {raw_date!r} — pomijam")
            return filtered
        except Exception as e:
            log.error(f"Błąd pobierania danych konkurencji z Sheets: {e}")
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

    def _save_json(self, data: list[dict], comp_data: list[dict], name: str, folder_key: str):
        try:
            summary = self._calculate_summary(data)
            comp_summary = self._calculate_competitor_summary(comp_data)
            output = {
                "generated_at": datetime.now().isoformat(),
                "pekao_tfi": {
                    "summary": summary,
                    "mentions": data,
                },
                "competitors": {
                    "summary": comp_summary,
                    "mentions": comp_data,
                },
            }
            path = f"/tmp/{name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=2)
            self._upload_to_drive(path, f"{name}.json", folder_key)
            log.info(f"JSON zapisany: {name}.json")
        except Exception as e:
            log.error(f"Błąd generowania JSON: {e}")

    def _save_pdf(self, data: list[dict], comp_data: list[dict], name: str, folder_key: str, title: str):
        try:
            path = f"/tmp/{name}.pdf"
            summary = self._calculate_summary(data)
            comp_summary = self._calculate_competitor_summary(comp_data)

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

            title_style = ParagraphStyle(
                "Title",
                fontSize=24,
                textColor=PEKAO_RED,
                spaceAfter=0.5*cm,
                fontName="Helvetica-Bold"
            )
            section_style = ParagraphStyle(
                "Section",
                fontSize=13,
                textColor=PEKAO_DARK,
                fontName="Helvetica-Bold",
                spaceAfter=0.3*cm,
                spaceBefore=0.3*cm,
            )
            subsection_style = ParagraphStyle(
                "Subsection",
                fontSize=11,
                textColor=PEKAO_DARK,
                fontName="Helvetica-Bold",
                spaceAfter=0.2*cm,
                spaceBefore=0.2*cm,
            )
            item_style = ParagraphStyle("Item", fontSize=10, spaceAfter=0.15*cm)
            desc_style = ParagraphStyle(
                "Desc", fontSize=9, textColor=HexColor("#555555"), spaceAfter=0.25*cm
            )

            # ── NAGŁÓWEK ──────────────────────────────────────────────────────
            story.append(Paragraph(f"Pekao TFI — {title}", title_style))
            story.append(Paragraph(
                f"Wygenerowano: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                styles["Normal"]
            ))
            story.append(Spacer(1, 0.5*cm))

            # ── SEKCJA 1: PEKAO TFI ──────────────────────────────────────────
            story.append(Paragraph("1. Pekao TFI — podsumowanie", section_style))

            pekao_summary_data = [
                ["Wskaźnik", "Wartość"],
                ["Liczba wzmianek", str(summary["total_mentions"])],
                ["Średni sentyment", f"{summary['avg_score']:.1f}/10"],
                ["Alerty kryzysowe", str(summary["crisis_count"])],
                ["Pochwały", str(summary["praise_count"])],
                ["Najczęstsze źródło", summary["top_source"]],
            ]
            story.append(self._make_table(pekao_summary_data))
            story.append(Spacer(1, 0.4*cm))

            # Wykres trendu Pekao TFI
            chart = self._build_trend_chart(data)
            if chart:
                story.append(Paragraph("Trend sentymentu — Pekao TFI", subsection_style))
                story.append(chart)
                story.append(Spacer(1, 0.4*cm))

            # Wzmianki Pekao TFI
            story.append(Paragraph(f"Wzmianki Pekao TFI (top 50 z {summary['total_mentions']})", subsection_style))
            story += self._render_mentions(data[:50], item_style, desc_style)

            # ── SEKCJA 2: ANALIZA KONKURENCJI ────────────────────────────────
            story.append(Spacer(1, 0.5*cm))
            story.append(Paragraph("2. Analiza konkurencji", section_style))

            # Tabela porównawcza
            if comp_summary:
                story.append(Paragraph("Porównanie sentymentu", subsection_style))
                comp_table_data = [
                    ["Firma", "Wzmianki", "Śr. sentyment", "Kryzysy (≤3)", "Pochwały (≥9)"],
                    [
                        "Pekao TFI",
                        str(summary["total_mentions"]),
                        f"{summary['avg_score']:.1f}/10",
                        str(summary["crisis_count"]),
                        str(summary["praise_count"]),
                    ],
                ]
                for comp_name, cs in sorted(comp_summary.items()):
                    comp_table_data.append([
                        comp_name,
                        str(cs["total_mentions"]),
                        f"{cs['avg_score']:.1f}/10",
                        str(cs["crisis_count"]),
                        str(cs["praise_count"]),
                    ])

                comp_table = Table(comp_table_data, colWidths=[4.5*cm, 2.5*cm, 3.5*cm, 3*cm, 3*cm])
                comp_table.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), PEKAO_RED),
                    ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#FFFFFF")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 10),
                    ("BACKGROUND", (0, 1), (0, 1), HexColor("#FFE5E5")),  # Pekao TFI row highlight
                    ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                    ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#CCCCCC")),
                    ("ROWBACKGROUNDS", (0, 2), (-1, -1), [HexColor("#FFFFFF"), PEKAO_GRAY]),
                    ("PADDING", (0, 0), (-1, -1), 7),
                ]))
                story.append(comp_table)
                story.append(Spacer(1, 0.4*cm))

            # Wykres porównawczy wieloliniowy
            comp_chart = self._build_competitor_chart(data, comp_data)
            if comp_chart:
                story.append(Paragraph("Trend sentymentu — porównanie", subsection_style))
                story.append(comp_chart)
                story.append(Spacer(1, 0.4*cm))

            # Wzmianki per konkurent
            competitors_in_data = sorted({r.get("competitor", "") for r in comp_data if r.get("competitor")})
            for comp_name in competitors_in_data:
                comp_items = [r for r in comp_data if r.get("competitor") == comp_name]
                cs = comp_summary.get(comp_name, {})
                avg = cs.get("avg_score", 0)
                story.append(Paragraph(
                    f"{comp_name} — {len(comp_items)} wzmianek, śr. sentyment {avg:.1f}/10",
                    subsection_style
                ))
                story += self._render_mentions(comp_items[:30], item_style, desc_style)
                story.append(Spacer(1, 0.3*cm))

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

    def _make_table(self, table_data: list[list]) -> Table:
        t = Table(table_data, colWidths=[8*cm, 8*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), PEKAO_RED),
            ("TEXTCOLOR", (0, 0), (-1, 0), HexColor("#FFFFFF")),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 11),
            ("GRID", (0, 0), (-1, -1), 0.5, HexColor("#CCCCCC")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [HexColor("#FFFFFF"), PEKAO_GRAY]),
            ("PADDING", (0, 0), (-1, -1), 8),
        ]))
        return t

    def _render_mentions(self, items: list[dict], item_style, desc_style) -> list:
        story = []
        for item in items:
            score = item.get("sentyment_koncowy", 5)
            try:
                score = float(score)
            except (TypeError, ValueError):
                score = 5.0
            color = "#CC0000" if score <= 3 else "#228B22" if score >= 9 else "#1A1A2E"
            styled = ParagraphStyle(
                "ItemDyn", parent=item_style, textColor=HexColor(color)
            )
            story.append(Paragraph(
                f"[{score:.0f}/10] {item.get('title', '')} "
                f"— {item.get('source', '')} "
                f"({str(item.get('date', ''))[:10]})",
                styled
            ))
            if item.get("podsumowanie"):
                story.append(Paragraph(item["podsumowanie"], desc_style))
        return story

    def _build_competitor_chart(self, pekao_data: list[dict], comp_data: list[dict]):
        """Wykres porównawczy sentymentu: Pekao TFI + wszyscy konkurenci."""
        try:
            COLORS = {
                "Pekao TFI": "#CC0000",
                "PKO TFI":   "#0055A4",
                "TFI PZU":   "#009900",
                "Goldman Sachs TFI": "#FFB300",
            }

            def daily_scores(records: list[dict], label: str) -> pd.DataFrame:
                rows = []
                for r in records:
                    raw_date = r.get("date", "")
                    score = r.get("sentyment_koncowy")
                    if not raw_date or score is None:
                        continue
                    try:
                        rows.append({
                            "date": datetime.fromisoformat(str(raw_date)),
                            "score": float(score),
                            "label": label,
                        })
                    except (ValueError, TypeError):
                        continue
                return pd.DataFrame(rows)

            frames = [daily_scores(pekao_data, "Pekao TFI")]
            competitors = sorted({r.get("competitor", "") for r in comp_data if r.get("competitor")})
            for comp in competitors:
                subset = [r for r in comp_data if r.get("competitor") == comp]
                frames.append(daily_scores(subset, comp))

            df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
            if df.empty:
                return None

            df["day"] = df["date"].dt.date
            fig, ax = plt.subplots(figsize=(14, 5))

            for label in df["label"].unique():
                sub = df[df["label"] == label]
                daily = sub.groupby("day")["score"].mean().reset_index()
                dates = [datetime.combine(d, datetime.min.time()) for d in daily["day"]]
                color = COLORS.get(label, "#888888")
                ax.plot(dates, daily["score"], color=color, linewidth=2,
                        marker="o", markersize=4, label=label)

            ax.axhline(y=3, color="#FFAAAA", linestyle="--", linewidth=1, label="Kryzys (3)")
            ax.axhline(y=9, color="#AAFFAA", linestyle="--", linewidth=1, label="Pochwała (9)")
            ax.set_ylim(1, 10)
            ax.set_ylabel("Sentyment (1–10)")
            ax.set_xlabel("Data")
            ax.legend(fontsize=8, loc="upper left")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m"))
            ax.xaxis.set_major_locator(mdates.DayLocator())
            plt.xticks(rotation=45)
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)

            return Image(buf, width=16*cm, height=5.5*cm)

        except Exception as e:
            log.error(f"Błąd generowania wykresu porównawczego: {e}")
            return None

    def _calculate_competitor_summary(self, comp_data: list[dict]) -> dict:
        """Zwraca słownik {nazwa_konkurenta: {total, avg, crisis, praise}}."""
        result = {}
        competitors = {r.get("competitor", "") for r in comp_data if r.get("competitor")}
        for comp in competitors:
            subset = [r for r in comp_data if r.get("competitor") == comp]
            scores = []
            for r in subset:
                val = r.get("sentyment_koncowy")
                if val is not None:
                    try:
                        scores.append(float(val))
                    except (TypeError, ValueError):
                        pass
            result[comp] = {
                "total_mentions": len(subset),
                "avg_score": round(sum(scores) / len(scores), 2) if scores else 0,
                "crisis_count": sum(1 for s in scores if s <= 3),
                "praise_count": sum(1 for s in scores if s >= 9),
            }
        return result

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
