from pacmandetections import Detection
from pacmandetections.risk import RiskAnalysis
import requests
import os
import logging


class PortalDetectionConnector:

    def __init__(self, endpoint="http://localhost:8000/api/detection/"):
        self.endpoint = endpoint

    def submit(self, items: list[Detection]):

        token = os.getenv("TOKEN_PACMAN_PORTAL")

        for item in items:
            res = requests.post(self.endpoint, json=item.to_dict(), headers={"Authorization": f"Token {token}"})
            if res.status_code > 201:
                logging.error(f"Failed to submit detection: {res.content}")


class PortalRiskAnalysisConnector:

    def __init__(self, endpoint="http://localhost:8000/api/risk_analysis/"):
        self.endpoint = endpoint

    def submit(self, items: list[RiskAnalysis]):

        token = os.getenv("TOKEN_PACMAN_PORTAL")

        for item in items:
            res = requests.post(self.endpoint, json=item.to_dict(), headers={"Authorization": f"Token {token}"})
            if res.status_code > 201:
                logging.error(f"Failed to submit risk analysis: {res.content}")
