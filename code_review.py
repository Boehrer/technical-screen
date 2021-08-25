import unittest

import pandas as pd


ETLS = {
    "us_spending": {
        "steps": [
            {
                "function": "download_csv",
                "args": {
                    "path": "https://www.usgovernmentspending.com/rev/usgs_downchart_csv.php?year=1990_2026&state=US&units=b&view=2&fy=fy21&chart=F0-fed&stack=1&local=s&thing=",
                    "header": 1
                }
            },
            {
                "function": "crop_csv",
                "args": {}
            }
        ]
    },
    "population_density": {
        "steps": [
            {
                "function": "download_csv",
                "args": {
                    "path": "http://data.un.org/_Docs/SYB/CSV/SYB63_1_202009_Population,%20Surface%20Area%20and%20Density.csv"
                }
            },
            {
                "function": "transform_csv",
                "args": {}
            }
        ]
    }
}


def download_csv(path, upstream_data, header=None):
    return pd.read_csv(path, header=header)


def crop_csv(upstream_data):
    return upstream_data.iloc[0:37]


def transform_csv(upstream_data):
    transformed_records = []
    for name, record in df.iterrows():
        record["foo"] = record["foo"] * 100
        transformed_records.append(record)
    return pd.DataFrame.from_records(transformed_records)


class Pipeline:

    def __init__(self, functions):
        self.functions = functions

    def run_step(self, step, upstream_data):
        """
        runs a step from an etl
        """
        function = self.functions[step["function"]]
        return function(**step["args"], upstream_data=upstream_data)

    def run_etl(self, etl):
        """
        runs all steps in an etl
        """
        data = None
        for step in etl["steps"]:
            data = self.run_etl(step, data)

    def run_etls(self, etls):
        """
        runs multiple etls
        """
        for etl in etls:
            for step in etl["steps"]:
                self.run_etl(step, data)


class TestPipeline(unittest.TestCase):
    pass


if __name__ == "__main__":
    pipeline = Pipeline(
        functions={
            "download_csv": download_csv,
            "transform_csv": transform_csv,
            "crop_csv": crop_csv
        }
    )
    for name, etl in ETL.items():
        print(f"running etl {name}")
        pipeline.run(etl)
