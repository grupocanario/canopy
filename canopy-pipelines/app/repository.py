from dagster import RepositoryDefinition, ScheduleDefinition
from pipelines import sobrecostos_pipeline


def pipeline_function(pipeline):
    return lambda: pipeline


pipeline_list = [
    sobrecostos_pipeline
]

pipeline_dict = {p.name: pipeline_function(p) for p in pipeline_list}


def define_repo():
    return RepositoryDefinition(
        name='wendy_pipelines_repo',
        pipeline_dict=pipeline_dict
    )
