from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
import dateutil.parser
from shapely import Geometry


class EstablishmentMeans(Enum):
    NATIVE = "native"
    INTRODUCED = "introduced"
    UNCERTAIN = "uncertain"


class RiskLevel(Enum):
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Confidence(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Invasiveness(Enum):
    INVASIVE = "invasive"
    CONCERN = "of_concern"
    UNCERTAIN = "uncertain"


@dataclass
class Media:
    thumbnail: str


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
class Assessment:

    establishmentMeans: EstablishmentMeans


@dataclass
class Detection:

    taxon: int
    scientificName: str
    h3: str
    date: str
    occurrences: list[Occurrence]
    area: int
    target_gene: str
    confidence: Confidence
    media: list[Media]

    def __repr__(self):
        description = f"Potential detection of {self.scientificName} with confidence {self.confidence.value} on {self.occurrences[0].get_day()}"
        if len(self.occurrences) > 0:
            occurrence = self.occurrences[0]
            if occurrence.materialSampleID is not None:
                description += f" in material sample {occurrence.materialSampleID}"
            if occurrence.datasetName is not None:
                description += f", dataset {occurrence.datasetName}"
            if occurrence.target_gene is not None:
                description += f", marker {occurrence.target_gene}"
        return description

    def to_dict(self):
        return {
            "taxon": self.taxon,
            "area": self.area,
            "h3": self.h3,
            "date": self.date,
            "occurrences": [occurrence.__dict__ for occurrence in self.occurrences],
            # "establishmentMeans": self.establishmentMeans.value,
            "target_gene": self.target_gene,
            "description": self.__repr__(),
            "confidence": self.confidence.value,
            "media": [media.__dict__ for media in self.media] if self.media else None,
        }


class Source(ABC):

    @abstractmethod
    def fetch(self, shape: Geometry, start_date, end_date) -> Occurrence:
        pass


@dataclass
class RiskAnalysis:
    taxon: int
    area: int
    date: str
    software: str
    software_version: str
    description: str
    records: int
    min_year: int
    max_year: int
    establishmentMeans_native: bool
    establishmentMeans_introduced: bool
    invasiveness_invasive: bool
    invasiveness_concern: bool
    thermal: bool
    global_impact: bool
    on_priority_list: bool
    risk_level: RiskLevel

    def __repr__(self):
        return f"Risk analysis for {self.taxon} in {self.area}"

    def to_dict(self):
        return {
            "taxon": self.taxon,
            "area": self.area,
            "date": self.date,
            "software": self.software,
            "software_version": self.software_version,
            "records": self.records,
            "min_year": self.min_year,
            "max_year": self.max_year,
            "establishmentMeans_native": self.establishmentMeans_native,
            "establishmentMeans_introduced": self.establishmentMeans_introduced,
            "invasiveness_invasive": self.invasiveness_invasive,
            "invasiveness_concern": self.invasiveness_concern,
            "global_impact": self.global_impact,
            "on_priority_list": self.on_priority_list,
            "thermal": self.thermal,
            "risk_level": self.risk_level.value,
            "description": self.__repr__()
        }
