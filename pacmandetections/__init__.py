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
from pacmandetections.model import Detection, EstablishmentMeans, Source, Occurrence, Confidence, Assessment, Invasiveness, Media
from pacmandetections.sources import OBISAPISource
from termcolor import colored
import re


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

    def perform_assessment(self, aphiaid: int) -> Assessment:

        establishmentMeans = None

        sp = Speedy(h3_resolution=7, data_dir=os.path.expanduser(self.speedy_data), cache_summary=True)
        summary = sp.get_summary(aphiaid, resolution=self.resolution, as_geopandas=False)

        summary_cell = summary[summary["h3"] == self.h3]
        assert len(summary_cell) <= 1

        if len(summary_cell) == 0:
            establishmentMeans = EstablishmentMeans.UNCERTAIN
        elif summary_cell["establishmentMeans_introduced"].any():
            establishmentMeans = EstablishmentMeans.INTRODUCED
        elif summary_cell["establishmentMeans_native"].any():
            establishmentMeans = EstablishmentMeans.NATIVE
        else:
            establishmentMeans = EstablishmentMeans.UNCERTAIN

        return Assessment(
            establishmentMeans=establishmentMeans,
        )

    def generate(self):

        occurrences = []
        end_date = datetime.today()
        start_date = end_date - timedelta(days=self.days)

        for source in self.sources:
            logging.info(f"Fetching data from {source}")
            source_occurrences = list(source.fetch(self.shape, start_date, end_date))
            if (len(source_occurrences)):
                color = "green"
            else:
                color = "red"
            logging.info(colored(f"Found {len(source_occurrences)} species occurrences between {start_date} and {end_date}", color))
            occurrences.extend(source_occurrences)

        detections = {}

        # get unique aphiaids across all occurrences

        all_aphiaids = set()

        for occurrence in occurrences:
            all_aphiaids.update(self.aphiaids_for_occurrence(occurrence).keys())

        logging.info(f"Found {len(all_aphiaids)} AphiaIDs in occurrence data")

        # only keep WRiMS aphiaids

        all_aphiaids = all_aphiaids.intersection(self.wrims)

        # check establishmentMeans, invasiveness, and global impact for each aphiaid

        assessments = dict()

        for i, aphiaid in enumerate(all_aphiaids):
            logging.info(colored(f"Performing assessment for AphiaID {aphiaid} ({i + 1} / {len(all_aphiaids)})", "blue"))
            assessments[aphiaid] = self.perform_assessment(aphiaid)

        # populate detections

        for occurrence in occurrences:

            aphiaids = self.aphiaids_for_occurrence(occurrence)

            # check establishmentMeans

            for i, aphiaid in enumerate(aphiaids.keys()):
                if aphiaid in all_aphiaids:

                    assessment = assessments[aphiaid]

                    if assessment.establishmentMeans == EstablishmentMeans.INTRODUCED or assessment.establishmentMeans == EstablishmentMeans.UNCERTAIN:
                        detection_key = f"{aphiaid}_{occurrence.target_gene}_{occurrence.get_day()}"
                        if detection_key not in detections:
                            detections[detection_key] = Detection(
                                taxon=aphiaid,
                                scientificName=self.wrims[aphiaid],
                                h3=self.h3,
                                date=occurrence.get_day(),
                                occurrences=[occurrence],
                                area=self.area,
                                target_gene=occurrence.target_gene,
                                confidence=aphiaids[aphiaid],
                                media=None
                            )
                        else:
                            detections[detection_key].occurrences.append(occurrence)

        # extract media from occurrence

        for key in detections:
            media = set()
            for occurrence in detections[key].occurrences:
                if occurrence.associatedMedia is not None:
                    urls = re.findall(r'(https?://[^\s]+)', occurrence.associatedMedia)
                    media.update(urls)
            if len(media) > 0:
                detections[key].media = [Media(thumbnail=url) for url in list(media)]

        return [detection for detection in detections.values()]
