import os
import requests
import logging

log = logging.getLogger(__name__)

class SlackNotifier:
    def __init__(self, config: dict):
        self.webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
        self.enabled = bool(self.webhook_url)

    def send(self, message: str, urgent: bool = False):
        if not self.enabled:
            log.info("Slack webhook nie skonfigurowany — pomijam")
            log.info(f"Wiadomość: {message}")
            return

        try:
            payload = {
                "text": message,
                "username": "Pekao TFI Monitor",
                "icon_emoji": "🚨" if urgent else "📊"
            }
            resp = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                log.info("Alert Slack wysłany pomyślnie")
            else:
                log.error(f"Błąd Slack: {resp.status_code}")
        except Exception as e:
            log.error(f"Błąd wysyłania Slack: {e}")
