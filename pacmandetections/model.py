from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
import dateutil.parser
from shapely import Geometry


class EstablishmentMeans(Enum):
    NATIVE = "native"
    INTRODUCED = "introduced"
    UNCERTAIN = "uncertain"


@dataclass
class Occurrence:
    scientificName: str
    AphiaID: int
    eventDate: str
    decimalLongitude: float
    decimalLatitude: float
    catalogNumber: str
    eventID: str
    materialSampleID: str
    establishmentMeans: str
    occurrenceRemarks: str
    associatedMedia: str
    datasetID: str
    datasetName: str
    target_gene: str
    DNA_sequence: str
    identificationRemarks: str

    def get_day(self):
        date = dateutil.parser.isoparse(self.eventDate)
        return date.strftime("%Y-%m-%d")


@dataclass
class Detection:

    taxon: int
    scientificName: str
    h3: str
    date: str
    occurrences: list[Occurrence]
    establishmentMeans: EstablishmentMeans
    area: int

    def __repr__(self):
        description = f"{self.scientificName} detected {self.establishmentMeans.value} on {self.occurrences[0].get_day()}"
        if len(self.occurrences) > 0:
            occurrence = self.occurrences[0]
            if occurrence.materialSampleID is not None:
                description += f" in material sample {occurrence.materialSampleID}"
            if occurrence.datasetName is not None:
                description += f", dataset {occurrence.datasetName}"
        return description

    def to_dict(self):
        return {
            "taxon": self.taxon,
            "area": self.area,
            "h3": self.h3,
            "date": self.date,
            "occurrences": [occurrence.__dict__ for occurrence in self.occurrences],
            "establishmentMeans": self.establishmentMeans.value,
            "description": self.__repr__()
        }


class Source(ABC):

    @abstractmethod
    def fetch(self, shape: Geometry, start_date, end_date) -> Occurrence:
        pass
