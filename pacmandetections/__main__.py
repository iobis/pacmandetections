from pacmandetections import DetectionEngine
from pacmandetections.risk import RiskEngine
from pacmandetections.connectors import PortalDetectionConnector, PortalRiskAnalysisConnector
from dotenv import load_dotenv
import logging
import importlib.resources


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def main():

    load_dotenv()

    # detections

    # engine = DetectionEngine(h3="859b41b3fffffff", speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*5, area=1)
    # detections = engine.generate()
    # connector = PortalConnector()
    # connector.submit(detections)

    # for h3 in ["8554b047fffffff", "8554b04ffffffff", "8554b043fffffff", "85574ddbfffffff"]:
    #     engine = DetectionEngine(h3=h3, speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*10, area=5)
    #     detections = engine.generate()
    #     connector = PortalDetectionConnector()
    #     connector.submit(detections)

    # risk

    with importlib.resources.open_text("pacmandetections.data", "wrims_aphiaids.txt") as f:
        lines = [line.strip().split("\t") for line in f.readlines()]
        wrims = {int(line[0].strip()): line[1] for line in lines}

    engine = RiskEngine(speedy_data="~/Desktop/werk/speedy/speedy_data", area=1, shape="POLYGON ((176.231689 -19.580493, 176.231689 -15.496032, 179.978027 -15.496032, 179.978027 -19.580493, 176.231689 -19.580493))")

    for taxon_id in wrims.keys():
        print(f"Calculating risk for {taxon_id}")
        analysis = engine.calculate_risk(taxon_id)
        connector = PortalRiskAnalysisConnector()
        connector.submit([analysis])


if __name__ == "__main__":
    main()
