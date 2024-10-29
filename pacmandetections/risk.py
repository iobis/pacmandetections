from shapely import Geometry, Polygon, from_wkt
from h3 import h3_to_geo_boundary, h3_get_resolution
from datetime import datetime, timedelta
import importlib.resources
from speedy import Speedy
import os
import json
from pacmandetections.util import aphiaid_from_lsid
import logging
from termcolor import colored
from pacmandetections.model import Detection, EstablishmentMeans, Source, Occurrence, RiskAnalysis, RiskLevel
from pacmandetections.sources import OBISAPISource
from h3pandas.util.shapely import polyfill
import geopandas as gpd
import duckdb
import pandas as pd
import numpy as np
import requests


class RiskEngine:

    def __init__(self, shape: Geometry | str, area: int = None, speedy_data: str = None):

        self.resolution = 5

        if isinstance(shape, str):
            self.shape = from_wkt(shape)
            # TODO: handle dateline wrap
        else:
            self.shape = shape
        self.h3 = pd.DataFrame({"h3": list(polyfill(self.shape, self.resolution, geo_json=True))})

        self.area = area
        self.speedy_data = speedy_data

        self.fetch_priority_lists()

    def fetch_priority_lists(self):

        res = requests.get(f"http://127.0.0.1:8000/api/priority_list?area={self.area}") 
        data = res.json()
        taxa_ids = []
        for entry in data:
            taxa_ids.extend(entry["taxa"])
        self.priority_taxa = set(taxa_ids)

    def summarize(self, summary: pd.DataFrame, envelope: pd.DataFrame) -> pd.DataFrame:

        # handle missing envelope
        if envelope is not None:
            envelope["thermal"] = True
        else:
            envelope_columns = {
                "h3": "string",
                "thermal": "bool"
            }
            envelope = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in envelope_columns.items()})

        conn = duckdb.connect()
        conn.register("summary", summary)
        conn.register("envelope", envelope)
        conn.register("cells", self.h3)

        aggregated = conn.execute("""
            select
                max(source_obis) as source_obis,
                max(source_gbif) as source_gbif,
                sum(records) as records,
                min(min_year) as min_year,
                max(max_year) as max_year,
                coalesce(max(establishmentMeans_native), false) as establishmentMeans_native,
                coalesce(max(establishmentMeans_introduced), false) as establishmentMeans_introduced,
                coalesce(max(invasiveness_invasive), false) as invasiveness_invasive,
                coalesce(max(invasiveness_concern), false) as invasiveness_concern,
                coalesce(max(thermal), false) as thermal,
            from cells
            left join envelope on envelope.h3 = cells.h3
            left join summary on summary.h3 = cells.h3
        """).fetchdf()

        return aggregated.to_dict(orient="index").get(0)

    def calculate_risk(self, aphiaid: int) -> RiskAnalysis:

        sp = Speedy(h3_resolution=7, data_dir=os.path.expanduser(self.speedy_data), cache_summary=True)
        summary = sp.get_summary(aphiaid, resolution=self.resolution, as_geopandas=False)
        envelope = sp.get_thermal_envelope(aphiaid, resolution=self.resolution, as_geopandas=False)

        aggregated = self.summarize(summary, envelope)
        global_impact = bool(summary.invasiveness_invasive.any())
        on_priority_list = aphiaid in self.priority_taxa

        risk_analysis = RiskAnalysis(
            taxon=aphiaid,
            area=self.area,
            date=datetime.now().isoformat(),
            software="pacmandetections",
            software_version=None,
            description=None,
            records=None if np.isnan(aggregated["records"]) else int(aggregated["records"]),
            min_year=None if np.isnan(aggregated["min_year"]) else int(aggregated["min_year"]),
            max_year=None if np.isnan(aggregated["max_year"]) else int(aggregated["max_year"]),
            establishmentMeans_native=aggregated["establishmentMeans_native"],
            establishmentMeans_introduced=aggregated["establishmentMeans_introduced"],
            invasiveness_invasive=aggregated["invasiveness_invasive"],
            invasiveness_concern=aggregated["invasiveness_concern"],
            thermal=aggregated["thermal"],
            global_impact=global_impact,
            on_priority_list=on_priority_list,
            risk_level=None
        )

        risk_level = None

        if on_priority_list:
            risk_level = RiskLevel.HIGH
        else:
            if aggregated["establishmentMeans_native"]:
                risk_level = RiskLevel.NONE
            elif aggregated["establishmentMeans_introduced"]:
                if aggregated["invasiveness_invasive"] or aggregated["invasiveness_concern"]:
                    risk_level = RiskLevel.HIGH
                elif global_impact:
                    risk_level = RiskLevel.HIGH
                else:
                    risk_level = RiskLevel.MEDIUM
            elif aggregated["thermal"]:
                if global_impact:
                    risk_level = RiskLevel.MEDIUM
                else:
                    risk_level = RiskLevel.LOW
            else:
                risk_level = RiskLevel.LOW

        risk_analysis.risk_level = risk_level

        return risk_analysis
