import unittest

import pandas as pd


DATASOURCES = {
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
                "function": "transform_csv",
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


def transform_csv(upstream_data):
    return upstream_data.iloc[0:37]


class Pipeline:

    def __init__(self, functions):
        self.functions = functions

    def run(self, datasource):
        """
        runs a datasource's steps
        """
        data = None
        for step in source["steps"]:
            data = self.run_step(step, data)

    def run_step(self, step, upstream_data):
        """
        runs a step from a datasource
        """
        function = self.functions[step["function"]]
        return function(**step["args"], upstream_data=upstream_data)

    def run_multiple(self, datasources):
        """
        runs multiple datasource's steps
        """
        for source in datasources:
            for step in source["steps"]:
                self.run_step(step, data)


class TestPipeline(unittest.TestCase):
    pass


if __name__ == "__main__":
    pipeline = Pipeline(
        functions={
            "download_csv": download_csv,
            "transform_csv": transform_csv,
        }
    )
    for name, source in DATASOURCES.items():
        print(f"running {name}")
        pipeline.run(source)
