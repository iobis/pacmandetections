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
from pacmandetections.model import Detection, EstablishmentMeans, Source, Occurrence, Confidence, Assessment, Invasiveness, Media, Evidence
from pacmandetections.sources import OBISAPISource
from termcolor import colored
import re
from itertools import chain
from collections import defaultdict


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

        self.load_wrims_ids()

    def load_wrims_ids(self) -> None:

        with importlib.resources.open_text("pacmandetections.data", "wrims_aphiaids.txt") as f:
            lines = [line.strip().split("\t") for line in f.readlines()]
            self.wrims = {int(line[0].strip()): line[1] for line in lines}

    def evidence_for_occurrence(self, occurrence: Occurrence) -> list[Evidence]:

        evidences = []

        # main identification

        evidence = Evidence(
            AphiaID=occurrence.AphiaID,
            target_gene=occurrence.target_gene,
            organismQuantity=occurrence.organismQuantity,
            identity=None,
            query_cover=None,
            method=None,
            date=occurrence.get_day(),
            occurrence=occurrence,
            alternatives=None
        )
        evidences.append(evidence)

        # identificationRemarks

        if occurrence.identificationRemarks is not None:
            try:
                remarks = json.loads(occurrence.identificationRemarks)
                if "annotations" in remarks:
                    for annotation in remarks["annotations"]:
                        # TODO: annotations currently do not have scientificNameID!
                        if "scientificNameID" in annotation:
                            aphiaid = aphiaid_from_lsid(annotation["scientificNameID"])
                            evidence = Evidence(
                                AphiaID=aphiaid,
                                target_gene=occurrence.target_gene,
                                organismQuantity=occurrence.organismQuantity,
                                identity=annotation.get("identity"),
                                query_cover=annotation.get("query_cover"),
                                method=annotation.get("method"),
                                date=occurrence.get_day(),
                                occurrence=occurrence,
                                alternatives=None
                            )
                            evidences.append(evidence)
            except json.JSONDecodeError:
                pass

        return evidences

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

    def fetch_occurrences(self):
        """Fetch occurrences from the registered sources."""

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

        return occurrences

    def keep_evidence(self, evidence: Evidence, check_wrims=True, assessments: dict[int, Assessment] = None) -> bool:
        if check_wrims and evidence.AphiaID not in self.wrims:
            return False
        if assessments and not (assessments[evidence.AphiaID].establishmentMeans == EstablishmentMeans.INTRODUCED or assessments[evidence.AphiaID].establishmentMeans == EstablishmentMeans.UNCERTAIN):
            return False
        if evidence.method == "VSEARCH" and evidence.identity < 0.99:
            return False
        return True

    def sort_evidence(self, evidences: list[Evidence]) -> list[Evidence]:
        return sorted(
            evidences,
            key=lambda x: (
                x.identity is None,
                -x.identity if x.identity is not None else float("-inf"),
                x.organismQuantity is None,
                -x.organismQuantity if x.organismQuantity is not None else float("-inf")
            )
        )

    def generate(self):
        """Generate detections."""

        occurrences = self.fetch_occurrences()

        # get evidence

        evidences = list(chain.from_iterable(self.evidence_for_occurrence(occurrence) for occurrence in occurrences))

        # first filtering pass (percent identity)

        evidences = [evidence for evidence in evidences if self.keep_evidence(evidence, check_wrims=False, assessments=None)]

        # after first filtering, determine alternative identifications per evidence
        # count aphiaids per occurrence, then add count to evidences

        occurrence_aphiaids = defaultdict(set[int])

        for evidence in evidences:
            occurrence_aphiaids[evidence.occurrence.id].add(evidence.AphiaID)

        for evidence in evidences:
            evidence.alternatives = len(occurrence_aphiaids[evidence.occurrence.id])

        # second filtering pass (WRiMS)

        evidences = [evidence for evidence in evidences if self.keep_evidence(evidence, check_wrims=True, assessments=None)]

        # collect risk assessments

        aphiaids = set(evidence.AphiaID for evidence in evidences)
        assessments = dict()
        for i, aphiaid in enumerate(aphiaids):
            logging.info(colored(f"Performing assessment for AphiaID {aphiaid} ({i + 1} / {len(aphiaids)})", "blue"))
            assessments[aphiaid] = self.perform_assessment(aphiaid)

        # third filtering pass and group by detection key

        evidences = [evidence for evidence in evidences if self.keep_evidence(evidence, check_wrims=True, assessments=assessments)]

        # group by detection key

        grouped_evidence = defaultdict(list)
        for evidence in evidences:
            grouped_evidence[evidence.get_key()].append(evidence)

        # generate detections

        detections = list()

        for detection_key in grouped_evidence:

            evidences = self.sort_evidence(grouped_evidence[detection_key])

            # collect occurrences

            occurrence_ids = set()
            occurrences = []
            for evidence in evidences:
                if evidence.occurrence.id not in occurrence_ids:
                    occurrences.append(evidence.occurrence)
                    occurrence_ids.add(evidence.occurrence.id)

            # create detection

            detection = Detection(
                h3=self.h3,
                area=self.area,
                taxon=evidences[0].AphiaID,
                scientificName=self.wrims[evidences[0].AphiaID],
                date=evidences[0].date,
                target_gene=evidences[0].target_gene,
                best_identity=evidences[0].identity,
                best_organismQuantity=evidences[0].organismQuantity,
                best_query_cover=evidences[0].query_cover,
                best_alternatives=evidences[0].alternatives,
                occurrences=occurrences,
                confidence=None,
                media=None
            )

            # calculate confidence

            if not detection.target_gene:
                detection.confidence = Confidence.HIGH
            elif detection.target_gene == "COI":
                if detection.best_organismQuantity < 10 or detection.best_alternatives > 2 or detection.best_identity is None:
                    detection.confidence = Confidence.LOW
                else:
                    detection.confidence = Confidence.MEDIUM
            elif detection.target_gene == "18S":
                if detection.best_organismQuantity < 10 or detection.best_alternatives > 2 or detection.best_identity is None:
                    detection.confidence = Confidence.LOW
                else:
                    detection.confidence = Confidence.MEDIUM
            else:
                detection.confidence = Confidence.LOW

            detections.append(detection)

        # extract media from occurrence

        for detection in detections:
            media = set()
            for occurrence in detection.occurrences:
                if occurrence.associatedMedia is not None:
                    urls = re.findall(r'(https?://[^\s]+)', occurrence.associatedMedia)
                    media.update(urls)
            if len(media) > 0:
                detection.media = [Media(thumbnail=url) for url in list(media)]

        return detections
