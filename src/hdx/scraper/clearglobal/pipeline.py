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
    representivity_mapping = {
        "high": "Representative survey at 95% confidence level and a 10% margin of error, or better",
        "moderate": "Representative survey, but less than 95% confidence level and/or greater than a 10% margin of error",
        "low": "Non-representative/indicative survey",
        "very_low": "Small scale, non-representative survey",
    }

    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._baseurl = self._configuration.get("base_url")
        self._start_date = default_enddate
        self._end_date = default_date
        self._dataset_sources = set()
        self._representivity_ratings = set()

    def get_locations(self):
        url = f"{self._baseurl}locations/"
        parameters = {
            "page_size": 500,
            "conds": [["location_level", "=", 0]],
            "flag": "published",
        }
        json = self._retriever.download_json(url, post=True, parameters=parameters)
        return json["data"]

    def get_pages(self, countryiso3: str, aggregation: int) -> List:
        all_data = []
        url = f"{self._baseurl}location/{countryiso3}"
        page = 0
        headers = ", ".join(self._configuration["headers"])
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

    def add_resources(self, countryiso3: str, dataset: Dataset) -> Optional[datetime]:
        latest_creation = default_date

        has_resources = False
        for aggregation in range(0, 3):
            data = self.get_pages(countryiso3, aggregation)
            if not data:
                continue
            for row in data:
                self._dataset_sources.add(row["source"])
                published = parse_date(row["datetime_published"])
                if published < self._start_date:
                    self._start_date = published
                if published > self._end_date:
                    self._end_date = published
                creation = parse_date(row["date_creation"])
                if creation > default_date:
                    latest_creation = creation
                self._representivity_ratings.add(row["representivity_rating"])
            filename = f"clearglobal_language_use_{countryiso3}_admin{aggregation}.csv"
            description = [f"Languages used in {countryiso3}"]
            if aggregation != 0:
                description.append(f" by Admin {aggregation}")
            resourcedata = {"name": filename, "description": "".join(description)}
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
        dataset.preview_off()
        return latest_creation

    def generate_dataset(self, state: Dict, countryiso3: str) -> Optional[Dataset]:
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

        last_modified = self.add_resources(countryiso3, dataset)
        if not last_modified:
            return None
        if last_modified > state.get(countryiso3, state["DEFAULT"]):
            state[countryiso3] = last_modified
        else:
            return None
        dataset.add_tag("languages")
        dataset.set_subnational(True)
        dataset.set_time_period(self._start_date, self._end_date)
        dataset_sources = ", ".join(self._dataset_sources)
        no_rep_ratings = len(self._representivity_ratings)
        if no_rep_ratings == 0:
            logger.error("No representivity rating, skipping")
            return None
        elif no_rep_ratings != 1:
            logger.error("Multiple representivity ratings, skipping")
            return None
        representivity_rating = self._representivity_ratings.pop()
        if representivity_rating == "very_high":
            dataset["methodology"] = "Census"
        else:
            dataset["methodology"] = "Other"
            dataset["methodology_other"] = self.representivity_mapping[
                representivity_rating
            ]
        description = self._configuration["description"].format(
            countryname=countryname, dataset_sources=dataset_sources
        )
        dataset["notes"] = description
        return dataset
