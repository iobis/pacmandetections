from pacmandetections import DetectionEngine
from pacmandetections.connectors import PortalConnector
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def main():

    load_dotenv()

    engine = DetectionEngine(h3="859b41b3fffffff", speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*2, area=1)
    detections = engine.generate()
    connector = PortalConnector()
    connector.submit(detections)


if __name__ == "__main__":
    main()
