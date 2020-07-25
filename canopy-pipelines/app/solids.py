import os
from dagster import (
    solid,
    Field,
    Enum,
    EnumValue,
    Failure,
    InputDefinition,
    Noneable,
    Nothing,
    OutputDefinition,
    Permissive,)
from dagster_shell.utils import execute
import pandas as pd
from custom_types import PandasDataFrame
from utils import without_keys
from datetime import datetime
from alarms.costs import ReferenceItemMatcher
from text_processing import TextPreprocessor


def shell_solid_config():
    return {
        'env': Field(
            Noneable(Permissive()),
            default_value=os.environ.copy(),
            is_required=False,
            description='An optional dict of environment variables to pass to the subprocess. '
            'Defaults to using os.environ.copy().',
        ),
        'output_logging': Field(
            Enum(
                name='OutputType',
                enum_values=[
                    EnumValue('STREAM', description='Stream script stdout/stderr.'),
                    EnumValue(
                        'BUFFER',
                        description='Buffer shell script stdout/stderr, then log upon completion.',
                    ),
                    EnumValue('NONE', description='No logging'),
                ],
            ),
            is_required=False,
            default_value='STREAM',
        ),
        'cwd': Field(
            Noneable(str),
            default_value=None,
            is_required=False,
            description='Working directory in which to execute shell script',
        ),
    }


@solid(
        input_defs=[InputDefinition('start', Nothing)],
        output_defs=[OutputDefinition(str, 'result')],
        config_schema={
            **shell_solid_config(),
            'from_date': Field(str, is_required=True)
        },
)
def collect_releases(context):
    app_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(app_dir, 'shell', 'kingfisher-collect.sh')

    from_date = context.solid_config.get('from_date')

    shell_command = 'bash {} {}'.format(
        script_path,
        from_date)

    output, return_code = execute(
        shell_command=shell_command, log=context.log, **without_keys(context.solid_config, ['from_date'])
    )

    if return_code:
        raise Failure(
            description='Shell command execution failed with output: {output}'.format(
                output=output
            )
        )

    return output


@solid(
        input_defs=[InputDefinition('start', Nothing)],
        output_defs=[OutputDefinition(str, 'result')],
        config_schema={
            **shell_solid_config(),
            'collection_id': Field(int, is_required=True)
        },
)
def process_releases(context):
    app_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(app_dir, 'shell', 'kingfisher-process.sh')

    collection_id = context.solid_config.get('collection_id')

    shell_command = 'bash {} {}'.format(
        script_path,
        collection_id)

    output, return_code = execute(
        shell_command=shell_command, log=context.log, **without_keys(context.solid_config, ['collection_id'])
    )

    if return_code:
        raise Failure(
            description='Shell command execution failed with output: {output}'.format(
                output=output
            )
        )

    return output


@solid(
        input_defs=[InputDefinition('start', Nothing)],
        output_defs=[OutputDefinition(str, 'result')],
        config_schema={
            **shell_solid_config(),
            'view_name': Field(str, is_required=True)
        },
)
def refresh_views(context):
    app_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(app_dir, 'shell', 'kingfisher-views.sh')

    view_name = context.solid_config.get('view_name')

    shell_command = 'bash {} {}'.format(
        script_path,
        view_name)

    output, return_code = execute(
        shell_command=shell_command, log=context.log, **without_keys(context.solid_config, ['view_name'])
    )

    if return_code:
        raise Failure(
            description='Shell command execution failed with output: {output}'.format(
                output=output
            )
        )

    return output


# Items Alarm
@solid
def load_items_reference_db(context):
    precios = pd.read_pickle('https://storage.googleapis.com/secop_data/precios_reference.pickle')
    return precios


@solid(
    config_schema={
        'score_cutoff': Field(
            int,
            default_value=70,
            is_required=False,
            description='Cutoff of fuzzy matching')
    }
)
def process_alarm_sobrecosto(context, secop_join: PandasDataFrame, precios: PandasDataFrame):
    secop_join['items_per_contract'] = secop_join.groupby('id_contrato')['item_description'].transform('count')
    secop_join_valid = secop_join.loc[lambda x: ~(x.items_per_contract == 1) | ~(x.item_quantity == 1)]
    secop_join_valid = secop_join_valid.loc[lambda x: x.tipo_de_contrato.isin(['Compraventa', 'Suministros'])]

    secop_join_valid['item_description'] = secop_join_valid.item_description.str.lower()
    precios['producto'] = precios.producto.str.lower()

    preprocessor = TextPreprocessor()
    secop_join_valid['item_description_clean'] = preprocessor.preprocess(secop_join_valid.item_description)
    precios['producto_clean'] = preprocessor.preprocess(precios.producto)

    item_matcher = ReferenceItemMatcher(precios.producto_clean, score_cutoff=0)

    item_matches = secop_join_valid.item_description_clean.apply(item_matcher.find_match)
    item_matches_df = pd.DataFrame(item_matches.tolist(), columns=['closest_match', 'score', 'match_id'])
    item_matches_df = item_matches_df.loc[lambda x: x.score >= context.solid_config.get('score_cutoff')]

    item_matches_df = (
        pd.merge(item_matches_df, precios, left_on='match_id', right_on='id')
            .drop(columns=['closest_match', 'id'])
    )

    secop_join_valid_w_match = secop_join_valid.join(item_matches_df, how='left')
    secop_join_valid_w_match = secop_join_valid_w_match.assign(
        alarma_sobrecosto=lambda x: x.item_price > x.precio_techo
    )

    return item_matches_df


@solid
def save_alarm_sobrecosto(context, item_matches: PandasDataFrame):
    # TODO save in db
    pass
