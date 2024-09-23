from pacmandetections import DetectionEngine
from pacmandetections.risk import RiskEngine
from pacmandetections.connectors import PortalDetectionConnector, PortalRiskAnalysisConnector
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def main():

    load_dotenv()

    # engine = DetectionEngine(h3="859b41b3fffffff", speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*5, area=1)
    # detections = engine.generate()
    # connector = PortalConnector()
    # connector.submit(detections)

    # for h3 in ["8554b047fffffff", "8554b04ffffffff", "8554b043fffffff", "85574ddbfffffff"]:
    #     engine = DetectionEngine(h3=h3, speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*10, area=5)
    #     detections = engine.generate()
    #     connector = PortalDetectionConnector()
    #     connector.submit(detections)

    engine = RiskEngine(speedy_data="~/Desktop/werk/speedy/speedy_data", area=1, shape="POLYGON ((176.231689 -19.580493, 176.231689 -15.496032, 179.978027 -15.496032, 179.978027 -19.580493, 176.231689 -19.580493))")

    for taxon_id in [505946,418723,208836,212506,107451,158417,397147,140483,367822,107414]:
    # for taxon_id in [505946]:
        analysis = engine.calculate_risk(taxon_id)
        connector = PortalRiskAnalysisConnector()
        connector.submit([analysis])


if __name__ == "__main__":
    main()
