import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self, config: dict):
        self.sender = os.environ.get("GMAIL_SENDER")
        self.password = os.environ.get("GMAIL_APP_PASSWORD")
        self.recipients = config["reports"]["email_recipients"]
        self.enabled = bool(self.sender and self.password)

    def send(self, message: str, urgent: bool = False):
        if not self.enabled:
            log.info("Email nie skonfigurowany — pomijam")
            return

        try:
            subject = (
                "🚨 ALERT KRYZYSOWY — Pekao TFI Monitor"
                if urgent else
                "🏆 POCHWAŁA — Pekao TFI Monitor"
            )

            msg = MIMEMultipart()
            msg["From"] = self.sender
            msg["To"] = ", ".join(self.recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(message, "plain", "utf-8"))

            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(self.sender, self.password)
                server.sendmail(
                    self.sender,
                    self.recipients,
                    msg.as_string()
                )
            log.info(f"Email wysłany do {self.recipients}")

        except Exception as e:
            log.error(f"Błąd wysyłania emaila: {e}")
