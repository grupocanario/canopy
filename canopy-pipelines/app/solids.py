from dagster import solid, Field
import requests
import pandas as pd
from custom_types import PandasDataFrame
from datetime import datetime
from alarms.costs import ItemFinderSecop2, ReferenceItemMatcher
from text_processing import TextPreprocessor


@solid
def load_secop_1_data(context):
    url_secop_i = 'https://www.datos.gov.co/resource/c82b-7jfi.json'
    p_secop_i = {'$limit': '1000000',
                 'anno_firma_del_contrato': '2020'}
    r_secop_i = requests.get(url_secop_i, params=p_secop_i)
    d_secop_i = r_secop_i.json()  # To .json
    secop_1 = pd.DataFrame(d_secop_i)  # To df

    return secop_1


@solid
def load_secop_2_data(context):
    url_secop_ii = "https://www.datos.gov.co/resource/jbjy-vk9h.json?$where=fecha_de_firma>='2020-01-01'"
    p_secop_ii = {'$limit': '1000000'}
    r_secop_ii = requests.get(url_secop_ii, params=p_secop_ii)
    d_secop_ii = r_secop_ii.json()  # To .json
    secop_2 = pd.DataFrame(d_secop_ii)  # To df

    return secop_2


@solid
def clean_secop_1(context, secop_1_raw):
    secop_1 = secop_1_raw
    secop_1['source'] = 'secop_2'

    return secop_1


def clean_secop_2(context, secop_2_raw):
    secop_2 = secop_2_raw
    secop_2['source'] = 'secop_2'

    return secop_2


def save_secop(context, secop_data: PandasDataFrame) -> PandasDataFrame:
    # select common columns
    pass


@solid
def extract_items_secop_2(context, secop_2: PandasDataFrame) -> PandasDataFrame:
    finder = ItemFinderSecop2()
    secop_2['items'] = secop_2.url.apply(lambda url: finder.find_items(url))

    return secop_2


# Items Alarm
@solid
def load_items_reference_db(context):
    precios = pd.read_pickle('https://storage.googleapis.com/secop_data/precios_reference.pickle')
    return precios


@solid(
    config={
        'batch_size': Field(
            int,
            default_value=100,
            is_required=False,
            description='Number of contracts to process')
    }
)
def load_secop_join(context):
    batch_size = context.solid_config.get('batch_size')

    context.log.info('loading secop items')
    secop_2_items = pd.read_pickle('https://storage.googleapis.com/secop_data/secop_2_covid_items.pickle')

    context.log.info('loading secop data')
    secop_2_covid = pd.read_pickle('https://storage.googleapis.com/secop_data/secop_2_covid.pickle')
    # secop_2_covid = secop_covid.loc[lambda x: (x.is_covid) & (x.source == 'secop_2')].copy()
    secop_2_covid = secop_2_covid.sample(n=batch_size)

    context.log.info('joining tables')
    secop_join = pd.merge(
        secop_2_covid,
        secop_2_items,
        how='left',
        on='id_contrato'
    )

    secop_join = secop_join[['nombre_entidad', 'nit_entidad', 'departamento', 'ciudad',
                             'proceso_de_compra', 'id_contrato',
                             'descripcion_del_proceso', 'tipo_de_contrato',
                             'modalidad_de_contratacion',
                             'documento_proveedor', 'proveedor_adjudicado',
                             'url',
                             'valor_del_contrato',
                             'item_code', 'item_description', 'item_quantity',
                             'item_unit', 'item_price']]

    return secop_join


@solid(
    config={
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
