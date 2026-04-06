#!/usr/bin/env python3
"""
Pekao TFI Media Monitor — Weekly Report Runner
Uruchamiany przez GitHub Actions co poniedziałek.
"""

import yaml
import logging
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from reports.report_generator import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)

def send_email_with_attachment(config, subject, body, attachment_path):
    try:
        sender = os.environ.get("GMAIL_SENDER")
        password = os.environ.get("GMAIL_APP_PASSWORD")
        recipients = config["reports"]["email_recipients"]

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = os.path.basename(attachment_path)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={filename}"
            )
            msg.attach(part)

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())

        log.info(f"Email tygodniowy wysłany do {recipients}")
    except Exception as e:
        log.error(f"Błąd wysyłania emaila: {e}")

def main():
    log.info("=== START: Raport Tygodniowy ===")
    config = load_config()

    generator = ReportGenerator(config)
    generator.generate_weekly()

    week = datetime.now().strftime("%Y-W%V")
    pdf_path = f"/tmp/raport_{week}.pdf"

    subject = f"📊 Raport Tygodniowy Pekao TFI — {week}"
    body = (
        f"W załączniku znajdziesz raport tygodniowy monitoringu PR Pekao TFI.\n\n"
        f"Okres: tydzień {week}\n"
        f"Wygenerowano: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Pełne dane dostępne w Google Sheets i Google Drive.\n\n"
        f"— Pekao TFI Media Monitor"
    )

    send_email_with_attachment(config, subject, body, pdf_path)
    log.info("=== KONIEC: Raport Tygodniowy ===")

if __name__ == "__main__":
    main()
