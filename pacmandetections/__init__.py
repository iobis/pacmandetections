from shapely import Geometry, Polygon
from h3 import h3_to_geo_boundary, h3_get_resolution
from datetime import datetime, timedelta
import importlib.resources
from speedy import Speedy
import os
import json
from pacmandetections.util import aphiaid_from_lsid
import logging
from termcolor import colored
from pacmandetections.model import Detection, EstablishmentMeans, Source, Occurrence, Confidence
from pacmandetections.sources import OBISAPISource


class DetectionEngine:

    def __init__(self, h3: Geometry | str, days: int = 365, sources: list[Source] = [OBISAPISource()], area: int = None, speedy_data: str = None):

        if isinstance(h3, str):
            coords = h3_to_geo_boundary(h3)
            flipped = tuple(coord[::-1] for coord in coords)
            self.shape = Polygon(flipped)
        else:
            if not isinstance(h3, Polygon):
                raise ValueError("h3 must be a shapely Polygon or a H3 string")
            self.shape = h3

        self.h3 = h3
        self.resolution = h3_get_resolution(h3)
        self.days = days
        self.sources = sources
        self.area = area
        self.speedy_data = speedy_data

        logging.info(f"Initializing detection engine for cell {self.h3} (resolution {self.resolution}) going back {self.days} days")

        self.load_wrims()

    def load_wrims(self) -> None:

        with importlib.resources.open_text("pacmandetections.data", "wrims_aphiaids.txt") as f:
            lines = [line.strip().split("\t") for line in f.readlines()]
            self.wrims = {int(line[0].strip()): line[1] for line in lines}

    def check_establishment(self, aphiaid: int) -> EstablishmentMeans:
        logging.debug(f"Checking establishment for {aphiaid}")

        sp = Speedy(h3_resolution=7, data_dir=os.path.expanduser(self.speedy_data), cache_summary=True)
        summary = sp.get_summary(aphiaid, resolution=self.resolution, as_geopandas=False)

        summary_cell = summary[summary["h3"] == self.h3]
        assert len(summary_cell) <= 1

        if len(summary_cell) == 0:
            return EstablishmentMeans.UNCERTAIN
        elif summary_cell["introduced"].any():
            return EstablishmentMeans.INTRODUCED
        elif summary_cell["native"].any():
            return EstablishmentMeans.NATIVE
        else:
            return EstablishmentMeans.UNCERTAIN

    def aphiaids_for_occurrence(self, occurrence: Occurrence) -> dict[int, Confidence]:

        aphiaids = dict()

        if occurrence.AphiaID is not None:
            if not occurrence.target_gene:
                aphiaids[occurrence.AphiaID] = Confidence.HIGH
            elif occurrence.target_gene == "COI":
                aphiaids[occurrence.AphiaID] = Confidence.MEDIUM
            else:
                aphiaids[occurrence.AphiaID] = Confidence.LOW

        # check identificationRemarks for other possible identifications

        if occurrence.identificationRemarks is not None:
            try:
                remarks = json.loads(occurrence.identificationRemarks)
                if "annotations" in remarks:
                    for annotation in remarks["annotations"]:
                        if "method" in annotation and "identity" in annotation and annotation["method"] == "VSEARCH" and annotation["identity"] >= 0.99:
                            if "scientificNameID" in annotation:
                                aphiaid = aphiaid_from_lsid(annotation["scientificNameID"])
                                if aphiaid is not None:
                                    if aphiaid not in aphiaids:
                                        aphiaids[aphiaid] = Confidence.LOW
            except json.JSONDecodeError:
                pass

        return aphiaids

    def generate(self):

        occurrences = []
        end_date = datetime.today()
        start_date = end_date - timedelta(days=self.days)

        for source in self.sources:
            logging.info(f"Fetching data from {source}")
            source_occurrences = list(source.fetch(self.shape, start_date, end_date))
            logging.info(f"Found {len(source_occurrences)} species occurrences between {start_date} and {end_date}")
            occurrences.extend(source_occurrences)

        detections = {}

        # get unique aphiaids across all occurrences

        all_aphiaids = set()

        for occurrence in occurrences:
            all_aphiaids.update(self.aphiaids_for_occurrence(occurrence).keys())

        logging.info(f"Found {len(all_aphiaids)} AphiaIDs in occurrence data")

        # only keep WRiMS aphiaids

        all_aphiaids = all_aphiaids.intersection(self.wrims)

        # check establishmentMeans for each aphiaid

        establishments = dict()

        for i, aphiaid in enumerate(all_aphiaids):
            logging.info(colored(f"Checking establishment for AphiaID {aphiaid} ({i + 1} / {len(all_aphiaids)})", "green"))
            establishments[aphiaid] = self.check_establishment(aphiaid)

        # populate detections

        for occurrence in occurrences:

            aphiaids = self.aphiaids_for_occurrence(occurrence)

            # check establishmentMeans

            for i, aphiaid in enumerate(aphiaids.keys()):
                if aphiaid in all_aphiaids:

                    establishment = establishments[aphiaid]

                    if establishment == EstablishmentMeans.INTRODUCED or establishment == EstablishmentMeans.UNCERTAIN:
                        detection_key = f"{aphiaid}_{occurrence.target_gene}_{occurrence.get_day()}"
                        if detection_key not in detections:
                            detections[detection_key] = Detection(
                                taxon=aphiaid,
                                scientificName=self.wrims[aphiaid],
                                h3=self.h3,
                                date=occurrence.get_day(),
                                occurrences=[occurrence],
                                establishmentMeans=establishment,
                                area=self.area,
                                target_gene=occurrence.target_gene,
                                confidence=aphiaids[aphiaid]
                            )
                        else:
                            detections[detection_key].occurrences.append(occurrence)

        return [detection for detection in detections.values()]
