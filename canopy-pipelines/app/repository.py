import os
import sys

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_PATH)

from dagster import repository, ScheduleDefinition  # noqa
from pipelines import (
    data_collection_pipeline)  # noqa


def pipeline_function(pipeline):
    return lambda: pipeline


pipeline_list = [
    data_collection_pipeline
]

# pipeline_dict = {p.name: pipeline_function(p) for p in pipeline_list}


@repository
def canopy_repo():
    return pipeline_list
