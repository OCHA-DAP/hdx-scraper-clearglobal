#!/usr/bin/python
"""CLEAR Global scraper"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.country import Country
from hdx.utilities.dateparse import default_date, default_enddate, parse_date
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    representivity_to_int = {
        "very_high": 0,
        "high": 1,
        "moderate": 2,
        "low": 3,
        "very_low": 4,
    }
    representivity_mapping = {
        0: "Census",
        1: "Representative survey at 95% confidence level and a 10% margin of error, or better",
        2: "Representative survey, but less than 95% confidence level and/or greater than a 10% margin of error",
        3: "Non-representative/indicative survey",
        4: "Small scale, non-representative survey",
    }

    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._baseurl = self._configuration.get("base_url")

    def get_locations(self, state: Dict):
        url = f"{self._baseurl}locations"
        parameters = {
            "page_size": 500,
            "conds": '[["location_level", "=", 0]]',
            "flag": "published",
            "fields": "date_creation,location_code",
        }
        json = self._retriever.download_json(url, parameters=parameters)
        locations = []
        for location in json["data"]:
            countryiso3 = location["location_code"]
            last_modified = parse_date(location["date_creation"])
            if last_modified > state.get(countryiso3, state["DEFAULT"]):
                state[countryiso3] = last_modified
                locations.append({"iso3": countryiso3})
        return locations

    def get_pages(self, countryiso3: str, aggregation: int) -> List:
        all_data = []
        url = f"{self._baseurl}location/{countryiso3}"
        page = 0
        headers = ",".join(self._configuration["headers"])
        while True:
            parameters = {
                "aggregation": aggregation,
                "page_size": 500,
                "page": page,
                "fields": headers,
            }
            filename = f"location_{countryiso3}_adm{aggregation}_{page}.json"
            json = self._retriever.download_json(
                url, filename=filename, parameters=parameters
            )
            data = json["data"]
            all_data.extend(data)
            if len(data) != 500 or len(data) == 0:
                return all_data
            page += 1

    def add_resources(
        self, countryiso3: str, countryname: str, dataset: Dataset
    ) -> Optional[datetime]:
        earliest_start_date = default_enddate
        latest_end_date = default_date
        dataset_sources = set()
        rep_rating_strs = set()
        has_resources = False

        for aggregation in range(0, 3):
            data = self.get_pages(countryiso3, aggregation)
            if not data:
                continue
            for row in data:
                dataset_sources.add(row["source"])
                published = parse_date(row["datetime_published"])
                if published < earliest_start_date:
                    earliest_start_date = published
                if published > latest_end_date:
                    latest_end_date = published
                rep_rating_strs.add(row["representivity_rating"])
            filename = f"clearglobal_language_use_{countryiso3}_admin{aggregation}.csv"
            description = [f"Languages used in {countryiso3}"]
            if aggregation != 0:
                description.append(f" by Admin {aggregation}")
                pcoded = True
            else:
                pcoded = False
            resourcedata = {
                "name": filename,
                "description": "".join(description),
                "p_coded": pcoded,
            }
            dataset.generate_resource_from_rows(
                self._tempdir,
                filename,
                data,
                resourcedata,
                headers=self._configuration["headers"],
            )
            has_resources = True
        if not has_resources:
            return None

        dataset.set_time_period(earliest_start_date, latest_end_date)
        no_rep_ratings = len(rep_rating_strs)
        if no_rep_ratings == 0:
            logger.error("No representivity rating, skipping")
            return None
        rep_rating_ints = []
        for rep_rating_str in rep_rating_strs:
            rep_rating_ints.append(self.representivity_to_int[rep_rating_str])
        methodology = []
        for rep_rating_int in sorted(rep_rating_ints):
            methodology.append(self.representivity_mapping[rep_rating_int])
        methodology_str = "; ".join(methodology)
        if methodology_str == "Census":
            dataset["methodology"] = "Census"
        else:
            dataset["methodology"] = "Other"
            dataset["methodology_other"] = methodology_str
        description = self._configuration["description"].format(
            countryname=countryname, dataset_sources=", ".join(dataset_sources)
        )
        dataset["notes"] = description

    def generate_dataset(self, countryiso3: str) -> Optional[Dataset]:
        countryname = Country.get_country_name_from_iso3(countryiso3)
        dataset_title = f"{countryname}: Languages"
        dataset_name = slugify(dataset_title)

        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        try:
            dataset.add_country_location(countryiso3)
        except HDXError:
            logger.error(f"Couldn't find country {countryiso3}, skipping")
            return None

        self.add_resources(countryiso3, countryname, dataset)
        dataset.add_tag("languages")
        dataset.set_subnational(True)
        dataset.preview_off()
        viz_url = self._configuration["viz_url"].format(countryname=countryname)
        dataset.set_custom_viz(viz_url.replace(" ", "%20"))
        return dataset
