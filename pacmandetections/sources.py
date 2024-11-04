from shapely import Geometry
from pyobis import occurrences
import pandas as pd
from pacmandetections.model import Occurrence, Source
import requests
from typing import Generator
from pacmandetections.util import try_float


class PyOBISSource(Source):

    def fetch(self, shape: Geometry, start_date, end_date) -> list[Occurrence]:

        required_cols = ["scientificName", "speciesid", "eventDate", "decimalLongitude", "decimalLatitude", "catalogNumber", "eventID", "materialSampleID", "establishmentMeans", "occurrenceRemarks", "associatedMedia", "datasetID", "datasetName", "target_gene", "DNAsequence", "identificationRemarks"]

        start_date_str = str(start_date)[0:10]
        end_date_str = str(end_date)[0:10]
        wkt = str(shape)
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
        return occ.apply(lambda row: Occurrence(*[None if pd.isna(x) else x for x in row]), axis=1)

    def __str__(self):
        return "OBIS (pyobis)"


class OBISAPISource(Source):

    def __init__(self):
        self.rank = "genus"

    def fetch(self, shape: Geometry, start_date, end_date) -> Generator[Occurrence, None, None]:

        start_date_str = str(start_date)[0:10]
        end_date_str = str(end_date)[0:10]
        wkt = str(shape)

        after = 0

        while True:
            url = f"https://api.obis.org/v3/occurrence?geometry={wkt}&startdate={start_date_str}&enddate={end_date_str}&after={after}&dna=true&size=10000"
            res = requests.get(url)
            results = res.json()["results"]
            if len(results) == 0:
                break
            rank_results = [record for record in results if record.get(f"{self.rank}id")]

            for result in rank_results:

                if dnas := result.get("dna"):
                    if len(dnas) > 0:
                        dna = dnas[0]
                        result["target_gene"] = dna.get("target_gene")
                        result["DNA_sequence"] = dna.get("DNA_sequence")

                occurrence = Occurrence(
                    id=result.get("id"),
                    scientificName=result.get("scientificName"),
                    AphiaID=result.get("speciesid"),
                    eventDate=result.get("eventDate"),
                    decimalLongitude=result.get("decimalLongitude"),
                    decimalLatitude=result.get("decimalLatitude"),
                    catalogNumber=result.get("catalogNumber"),
                    eventID=result.get("eventID"),
                    materialSampleID=result.get("materialSampleID"),
                    establishmentMeans=result.get("establishmentMeans"),
                    occurrenceRemarks=result.get("occurrenceRemarks"),
                    associatedMedia=result.get("associatedMedia"),
                    datasetID=result.get("datasetID"),
                    datasetName=result.get("datasetName"),
                    target_gene=result.get("target_gene"),
                    DNA_sequence=result.get("DNA_sequence"),
                    identificationRemarks=result.get("identificationRemarks"),
                    organismQuantity=try_float(result.get("organismQuantity"))
                )

                yield occurrence

            after = results[-1]["id"]

    def __str__(self):
        return "OBIS (API)"


class GBIFSource(Source):

    def fetch(self, shape: Geometry, start_date, end_date) -> list[Occurrence]:
        pass
