from pacmandetections import DetectionEngine
from pacmandetections.risk import RiskEngine
from pacmandetections.connectors import PortalDetectionConnector, PortalRiskAnalysisConnector
from dotenv import load_dotenv
import logging
import importlib.resources
import geopandas as gpd
from h3pandas.util.shapely import polyfill


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def detections():

    load_dotenv()
    connector = PortalDetectionConnector()

    area = connector.fetch_area(1)
    gs = gpd.GeoSeries.from_wkt([area.get("wkt")])
    cells = list(polyfill(gs[0], 5, geo_json=True))

    for cell in ["859b41b3fffffff"]:
        engine = DetectionEngine(h3=cell, speedy_data="~/Desktop/werk/speedy/speedy_data", days=365*5, area=1)
        detections = engine.generate()
        connector.submit(detections)


def risk():

    load_dotenv()

    with importlib.resources.open_text("pacmandetections.data", "wrims_aphiaids.txt") as f:
        lines = [line.strip().split("\t") for line in f.readlines()]
        wrims = {int(line[0].strip()): line[1] for line in lines}

    engine = RiskEngine(speedy_data="~/Desktop/werk/speedy/speedy_data", area=1, shape="POLYGON ((176.231689 -19.580493, 176.231689 -15.496032, 179.978027 -15.496032, 179.978027 -19.580493, 176.231689 -19.580493))")

    for taxon_id in wrims.keys():
        logging.info(f"Calculating risk for {taxon_id}")
        analysis = engine.calculate_risk(taxon_id)
        connector = PortalRiskAnalysisConnector()
        connector.submit([analysis])


def main():

    # risk()
    detections()


if __name__ == "__main__":
    main()
