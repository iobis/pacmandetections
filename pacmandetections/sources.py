from shapely import Geometry
from abc import ABC, abstractmethod
from pyobis import occurrences
from dataclasses import dataclass
import pandas as pd
import dateutil.parser


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

    def get_day(self):
        date = dateutil.parser.isoparse(self.eventDate)
        return date.strftime("%Y-%m-%d")


class Source(ABC):

    @abstractmethod
    def fetch(self, geometry: Geometry, start_date, end_date) -> Occurrence:
        pass


class OBISSource(Source):

    def fetch(self, geometry: Geometry, start_date, end_date) -> list[Occurrence]:

        required_cols = ["scientificName", "speciesid", "eventDate", "decimalLongitude", "decimalLatitude", "catalogNumber", "eventID", "materialSampleID", "establishmentMeans"]

        start_date_str = str(start_date)[0:10]
        end_date_str = str(end_date)[0:10]
        wkt = str(geometry)
        query = occurrences.search(geometry=wkt, startdate=start_date_str, enddate=end_date_str)
        query.execute()
        occ = query.data

        missing_cols = [col for col in required_cols if col not in occ.columns]
        for col in missing_cols:
            occ[col] = pd.Series(dtype="string")

        occ = occ[occ["speciesid"].notnull()]
        occ = occ[required_cols]
        occ = occ.rename(columns={"speciesid": "AphiaID"})
        occ["AphiaID"] = occ["AphiaID"].astype(int)
        return occ.apply(lambda row: Occurrence(*row), axis=1)


class GBIFSource(Source):

    def fetch(self, geometry: Geometry, start_date, end_date) -> list[Occurrence]:
        pass
