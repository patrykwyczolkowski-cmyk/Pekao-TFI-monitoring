import os
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)


class EmailNotifier:
    def __init__(self, config: dict):
        self.sender = os.environ.get("GMAIL_SENDER")
        self.password = os.environ.get("GMAIL_APP_PASSWORD")
        self.recipients = config["reports"]["email_recipients"]
        self.enabled = bool(self.sender and self.password)

    def send(self, message: str, urgent: bool = False):
        if not self.enabled:
            log.info("Email nie skonfigurowany (brak GMAIL_SENDER/GMAIL_APP_PASSWORD) — pomijam")
            return

        try:
            subject = (
                "ALERT KRYZYSOWY — Pekao TFI Monitor"
                if urgent else
                "POCHWALA — Pekao TFI Monitor"
            )
            self._send_raw(subject, message)
        except Exception as e:
            log.error(f"Błąd wysyłania emaila alertu: {e}")

    def send_daily_digest(self, stats: dict):
        """
        Wysyła codzienny digest niezależnie od liczby wzmianek.
        Zawsze wysyłany — nawet przy zerowych wynikach lub awarii.

        stats:
            date (str), run_time (str),
            pekao_scraped (int), pekao_new (int),
            pekao_analyzed (int), pekao_avg_sentiment (float),
            pekao_crisis (int), pekao_praise (int),
            pekao_mgmt_mentions (int),
            competitors (dict: name -> count),
            alerts_sent (list[str]),
            status (str: "OK" | "BLAD"),
            errors (list[str])
        """
        if not self.enabled:
            log.info("Email nie skonfigurowany — pomijam digest")
            return

        try:
            date_str = stats.get("date", datetime.now().strftime("%d.%m.%Y"))
            run_time = stats.get("run_time", datetime.now().strftime("%H:%M"))
            status = stats.get("status", "OK")

            status_icon = "OK" if status == "OK" else "BLAD"
            subject = f"[{status_icon}] Dzienny monitoring Pekao TFI — {date_str}"

            lines = [
                f"DZIENNY MONITORING — Pekao TFI",
                f"Data: {date_str}  |  Godzina uruchomienia: {run_time}",
                f"Status systemu: {status}",
                "",
                "=" * 50,
                "PEKAO TFI",
                "=" * 50,
            ]

            pekao_new = stats.get("pekao_new", 0)
            pekao_analyzed = stats.get("pekao_analyzed", 0)
            avg = stats.get("pekao_avg_sentiment")
            crisis = stats.get("pekao_crisis", 0)
            praise = stats.get("pekao_praise", 0)
            mgmt = stats.get("pekao_mgmt_mentions", 0)

            lines.append(f"Nowe wzmianki dzisiaj:    {pekao_new}")
            lines.append(f"Przeanalizowane przez AI:  {pekao_analyzed}")

            if avg is not None:
                lines.append(f"Sredni sentyment:         {avg:.1f}/10")
            if crisis:
                lines.append(f"Alerty kryzysowe (<=3):   {crisis}  <-- WYMAGA UWAGI")
            if praise:
                lines.append(f"Pochwalenia (>=9):         {praise}")
            if mgmt:
                lines.append(f"Wzmianki o zarzadzie:     {mgmt}")
            if pekao_new == 0:
                lines.append("(brak nowych wzmianek)")

            # Alerty wysłane
            alerts = stats.get("alerts_sent", [])
            if alerts:
                lines += ["", "Wysłane alerty:"]
                for a in alerts:
                    lines.append(f"  - {a}")

            # Konkurencja
            competitors = stats.get("competitors", {})
            if competitors:
                lines += ["", "=" * 50, "KONKURENCJA", "=" * 50]
                for name, count in competitors.items():
                    lines.append(f"  {name:<30} {count} wzmianek")

            # Blogi (jeśli osobno zliczane)
            blog_count = stats.get("blog_mentions", 0)
            if blog_count:
                lines += ["", f"Wzmianki z blogow inwestycyjnych: {blog_count}"]

            # Bledy
            errors = stats.get("errors", [])
            if errors:
                lines += ["", "=" * 50, "BLEDY SYSTEMU", "=" * 50]
                for e in errors:
                    lines.append(f"  ! {e}")

            lines += [
                "",
                "=" * 50,
                "Pekao TFI Media Monitor — raport automatyczny",
                "Raporty tygodniowe/miesięczne: Google Drive",
            ]

            body = "\n".join(lines)
            self._send_raw(subject, body)
            log.info(f"Daily digest wysłany do {self.recipients}")

        except Exception as e:
            log.error(f"Błąd wysyłania daily digest: {e}")

    def _send_raw(self, subject: str, body: str):
        msg = MIMEMultipart()
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(self.sender, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())
