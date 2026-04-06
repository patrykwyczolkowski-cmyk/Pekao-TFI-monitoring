import logging
from alerts.slack_notifier import SlackNotifier

log = logging.getLogger(__name__)

class AlertEngine:
    def __init__(self, config: dict, results: list[dict]):
        self.config = config
        self.results = results
        self.crisis_threshold = config["thresholds"]["crisis"]
        self.praise_threshold = config["thresholds"]["praise"]
        self.trend_drop = config["thresholds"]["trend_drop"]
        self.slack = SlackNotifier(config)

    def check_and_send(self):
        crisis_items = []
        praise_items = []

        for r in self.results:
            if not r.get("dotyczy_pekao_tfi"):
                continue

            score = r.get("sentyment_końcowy", 5)

            if score <= self.crisis_threshold:
                crisis_items.append(r)
            elif score >= self.praise_threshold:
                praise_items.append(r)

        if crisis_items:
            log.warning(f"KRYZYS: {len(crisis_items)} wzmianek!")
            self._send_crisis_alert(crisis_items)

        if praise_items:
            log.info(f"POCHWAŁA: {len(praise_items)} wzmianek!")
            self._send_praise_alert(praise_items)

        if not crisis_items and not praise_items:
            log.info("Brak alertów — wszystko w normie")

    def _send_crisis_alert(self, items: list[dict]):
        message = "🚨 *ALERT KRYZYSOWY — Pekao TFI*\n\n"
        for item in items:
            message += (
                f"*Źródło:* {item.get('source')}\n"
                f"*Tytuł:* {item.get('title')}\n"
                f"*Wynik:* {item.get('sentyment_końcowy')}/10\n"
                f"*Opis:* {item.get('podsumowanie')}\n"
                f"*Link:* {item.get('url')}\n\n"
            )
        self.slack.send(message, urgent=True)

    def _send_praise_alert(self, items: list[dict]):
        message = "🏆 *POCHWAŁA — Pekao TFI*\n\n"
        for item in items:
            message += (
                f"*Źródło:* {item.get('source')}\n"
                f"*Tytuł:* {item.get('title')}\n"
                f"*Wynik:* {item.get('sentyment_końcowy')}/10\n"
                f"*Opis:* {item.get('podsumowanie')}\n"
                f"*Link:* {item.get('url')}\n\n"
            )
        self.slack.send(message, urgent=False)
