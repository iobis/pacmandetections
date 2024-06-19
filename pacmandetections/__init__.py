from shapely import Geometry, Polygon
from pacmandetections.sources import Source, OBISSource, Occurrence
from h3 import h3_to_geo_boundary
from datetime import datetime, timedelta
import importlib.resources
from speedy import Speedy
import os
import h3
from enum import Enum


class EstablishmentMeans(Enum):
    NATIVE = "native"
    INTRODUCED = "introduced"
    UNCERTAIN = "uncertain"


class Detection:

    def __init__(self, occurrences: list[Occurrence], establishmentMeans: EstablishmentMeans):
        self.occurrences = occurrences
        self.establishmentMeans = establishmentMeans

    def __repr__(self):
        return f"{self.occurrences[0].scientificName} detected {self.establishmentMeans.value} on {self.occurrences[0].get_day()}"


class DetectionEngine:

    def __init__(self, geometry: Geometry | str, days: int = 365, sources: list[Source] = [OBISSource()], speedy_data: str = None):

        if isinstance(geometry, str):
            coords = h3_to_geo_boundary(geometry)
            flipped = tuple(coord[::-1] for coord in coords)
            geometry = Polygon(flipped)

        self.geometry = geometry
        self.days = days
        self.sources = sources
        self.speedy_data = speedy_data

        self.load_wrims()

    def load_wrims(self):

        with importlib.resources.open_text("pacmandetections.data", "wrims_aphiaids.txt") as f:
            self.aphiaids = f.readlines()

    def check_establishment(self, occurrence: Occurrence):

        sp = Speedy(h3_resolution=7, data_dir=os.path.expanduser(self.speedy_data))
        summary = sp.get_summary(occurrence.AphiaID, resolution=5, cached=True)
        cell = h3.geo_to_h3(occurrence.decimalLatitude, occurrence.decimalLongitude, 7)

        summary_cell = summary[summary["h3"] == cell]
        assert len(summary_cell) <= 1

        if len(summary_cell) == 0:
            return EstablishmentMeans.UNCERTAIN
        elif summary_cell["introduced"].any():
            return EstablishmentMeans.INTRODUCED
        elif summary_cell["natibe"].any():
            return EstablishmentMeans.NATIVE
        else:
            return EstablishmentMeans.UNCERTAIN

    def generate(self):

        occurrences = []
        end_date = datetime.today()
        start_date = end_date - timedelta(days=self.days)

        for source in self.sources:
            occurrence = source.fetch(self.geometry, start_date, end_date)
            occurrences.extend(occurrence)

        # check risk

        detections = {}

        for occurrence in occurrences:
            establishment = self.check_establishment(occurrence)
            if establishment == EstablishmentMeans.INTRODUCED or establishment == EstablishmentMeans.UNCERTAIN:
                detection_key = f"{occurrence.AphiaID}_{occurrence.get_day()}"
                if detection_key not in detections:
                    detections[detection_key] = Detection(occurrences=[occurrence], establishmentMeans=establishment)
                else:
                    detections[detection_key].occurrences.append(occurrence)

        return [detection for detection in detections.values()]
