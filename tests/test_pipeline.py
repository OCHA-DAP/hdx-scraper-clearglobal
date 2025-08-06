from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.clearglobal.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestCLEARGlobal",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                pipeline = Pipeline(configuration, retriever, tempdir)
                countries = pipeline.get_locations()
                assert len(countries) == 250
                assert countries[23] == {
                    "location_code": "BEN",
                    "location_id": 24,
                    "location_level": 0,
                    "location_name": "Benin",
                }
                creation = parse_date("2017-01-01")
                dataset = pipeline.generate_dataset({"DEFAULT": creation}, "BEN")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "caveats": None,
                    "data_update_frequency": -2,
                    "dataset_date": "[2013-12-31T00:00:00 TO 2013-12-31T23:59:59]",
                    "dataset_preview": "no_preview",
                    "dataset_source": "CLEAR Global",
                    "groups": [{"name": "ben"}],
                    "license_id": "cc-by-sa",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "methodology": "Census",
                    "name": "benin-languages",
                    "notes": "Data on languages spoken in Benin, showing the main language spoken "
                    "in the household by proportion of the population. Data is drawn "
                    "from IPUMS International. For more resources on the languages of "
                    "Benin and language use in humanitarian contexts please visit: "
                    "https://clearglobal.org/language-maps-and-data/",
                    "owner_org": "707b1f6d-5595-453f-8da7-01770b76e178",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "languages",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        }
                    ],
                    "title": "Benin: Languages",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Languages used in BEN",
                        "format": "csv",
                        "name": "clearglobal_language_use_BEN_admin0.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Languages used in BEN by Admin 1",
                        "format": "csv",
                        "name": "clearglobal_language_use_BEN_admin1.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Languages used in BEN by Admin 2",
                        "format": "csv",
                        "name": "clearglobal_language_use_BEN_admin2.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                for resource in resources:
                    filename = resource["name"]
                    actual = join(tempdir, filename)
                    expected = join(fixtures_dir, filename)
                    assert_files_same(actual, expected)
