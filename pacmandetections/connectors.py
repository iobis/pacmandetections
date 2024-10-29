from pacmandetections import Detection
from pacmandetections.risk import RiskAnalysis
import requests
import os
import logging


class PortalDetectionConnector:

    def __init__(self, endpoint="http://127.0.0.1:8000/api"):
        self.endpoint = endpoint
        self.token = os.getenv("TOKEN_PACMAN_PORTAL")

    def fetch_area(self, area_id: int) -> dict:
        res = requests.get(f"{self.endpoint}/area/{area_id}", headers={"Authorization": f"Token {self.token}"})
        area = res.json()
        return area

    def submit(self, items: list[Detection]):

        for item in items:
            res = requests.post(f"{self.endpoint}/detection/", json=item.to_dict(), headers={"Authorization": f"Token {self.token}"})
            if res.status_code > 201:
                logging.error(f"Failed to submit detection: {res.content}")
            else:
                logging.info("Detection submitted")


class PortalRiskAnalysisConnector:

    def __init__(self, endpoint="http://127.0.0.1:8000/api"):
        self.endpoint = endpoint
        self.token = os.getenv("TOKEN_PACMAN_PORTAL")

    def submit(self, items: list[RiskAnalysis]):

        for item in items:
            res = requests.post(f"{self.endpoint}/risk_analysis/", json=item.to_dict(), headers={"Authorization": f"Token {self.token}"})
            if res.status_code > 201:
                logging.error(f"Failed to submit risk analysis: {res.content}")
