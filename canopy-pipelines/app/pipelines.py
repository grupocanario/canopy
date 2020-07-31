from dagster import pipeline, ModeDefinition, PresetDefinition
from solids import (
    collect_releases, process_releases, log_data_collection
)
from resources import ocds_loader_resource


# Mode definitions
default_mode = ModeDefinition(
    name='default',
    resource_defs={
        'ocds_loader': ocds_loader_resource,
    }
)

default_preset = PresetDefinition(name='default_preset', mode='default')


@pipeline(
    mode_defs=[default_mode],
    preset_defs=[default_preset]
)
def data_collection_pipeline():
    log_data_collection(process_releases(collect_releases()))



