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


class RiskEngine:

    def __init__(self, shape: Geometry | str, area: int = None, speedy_data: str = None):

        self.resolution = 5

        if isinstance(shape, str):
            self.shape = from_wkt(shape)
            # todo: handle dateline wrap
        else:
            self.shape = shape
        self.h3 = pd.DataFrame({"h3": list(polyfill(self.shape, self.resolution, geo_json=True))})

        self.area = area
        self.speedy_data = speedy_data

    def summarize(self, summary: pd.DataFrame, envelope: pd.DataFrame) -> pd.DataFrame:
        envelope["thermal"] = True
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
                coalesce(max(native), false) as native,
                coalesce(max(introduced), false) as introduced,
                coalesce(max(uncertain), false) as uncertain,
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
        result = self.summarize(summary, envelope)

        risk_analysis = RiskAnalysis(
            taxon=aphiaid,
            area=self.area,
            date=datetime.now().isoformat(),
            software="pacmandetections",
            software_version=None,
            description=None,
            records=None if np.isnan(result["records"]) else int(result["records"]),
            min_year=None if np.isnan(result["min_year"]) else int(result["min_year"]),
            max_year=None if np.isnan(result["max_year"]) else int(result["max_year"]),
            native=result["native"],
            introduced=result["introduced"],
            uncertain=result["uncertain"],
            thermal=result["thermal"],
            risk_level=None
        )

        if result["introduced"]:
            risk_analysis.risk_level = RiskLevel.HIGH
        elif result["native"]:
            risk_analysis.risk_level = RiskLevel.LOW
        elif result["thermal"]:
            risk_analysis.risk_level = RiskLevel.MEDIUM
        else:
            risk_analysis.risk_level = RiskLevel.LOW

        return risk_analysis
