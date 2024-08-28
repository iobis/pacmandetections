from pacmandetections import Detection
import requests
import os
import logging


class PortalConnector:

    def __init__(self, endpoint="http://localhost:8000/api/detection/"):
        self.endpoint = endpoint

    def submit(self, detections: list[Detection]):

        token = os.getenv("TOKEN_PACMAN_PORTAL")

        for detection in detections:
            res = requests.post(self.endpoint, json=detection.to_dict(), headers={"Authorization": f"Token {token}"})
            if res.status_code != 200:
                logging.error(f"Failed to submit detection: {res.content}")
