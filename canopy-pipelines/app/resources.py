from dagster import resource, Field
from typing import NamedTuple, Dict
from dataset.util import DatasetException
import dataset
from sqlalchemy import literal_column
from sqlalchemy.sql import select, text
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


class ModelTableConfig(NamedTuple):
    model_table_name: str  # the table that holds the model results
    parent_key_col: str
    null_field_name: str  # how to determine after an antijoin whether the model is calculated


class SQLDBLoader:
    """
    Base clase of data loaders/savers to the postgres DB
    """

    DEFAULT_STATEMENT_TIMEOUT = 1800*1000  # half an hour (in milliseconds)

    def __init__(self, context, conn_str: str, statement_timeout: int):
        self.conn_str = conn_str
        self.statement_timeout = statement_timeout
        self.context = context
        self.log = self.context.log
        self.db = None

    def _get_db(self):
        if not self.db:
            self.db = dataset.connect(
                self.conn_str,
                ensure_schema=False,
                engine_kwargs={'connect_args': {'options': '-c statement_timeout={}'.format(self.statement_timeout)}}
            )
            self.log.info('acquired db connection')

            return self.db
        else:
            self.log.info('acquired exinsting db connection')

            return self.db

    def check_tables(self, *table_names, raise_exception=True):
        db = self.db

        for table_name in table_names:
            if table_name not in db:
                if raise_exception:
                    raise DatasetException("Table does not exist: %s" % table_name)
                else:
                    return False

        # if all tables in db
        return True

    def check_columns(self, table, data: pd.DataFrame, raise_exception=True):
        if not data.empty:
            columns_existence = np.array([table.has_column(col) for col in data.columns])

            all_columns_exist = np.all(columns_existence)
            failed_columns = data.columns[~columns_existence]

            if not all_columns_exist and raise_exception:
                raise DatasetException("Column(s) {} do not exist in table {}".format(str(failed_columns), str(table)))

            return all_columns_exist

    def _add_additional_filters(self, statement, additional_filters):
        if additional_filters:
            for additional_filter in additional_filters:
                statement = statement.where(text(additional_filter))

        return statement

    def _filter_non_null(self, statement, non_null_cols):
        if non_null_cols:
            for col in non_null_cols:
                statement = statement.where(col != None)  # noqa

        return statement

    def query_statement(self, db, statement):
        self.log.debug(str(statement))

        # execution of query by dataset
        data_iter = db.query(statement)
        data_records = list(data_iter)

        return pd.DataFrame(data_records)

    def table_from_sql(self, sql_query, params: Dict = None):
        """
        Obtains data records directly from executing a sql query
        :param sql_query:
        :param params:
        :return: selected cols from table when the associated record does not exist in the reference table
        """
        db = self._get_db()

        statement = text(sql_query)

        if params:
            statement = statement.bindparams(**params)

        df = self.query_statement(db, statement)

        return df

    def save_model_results(self, data, table_config: ModelTableConfig):
        table_name, parent_key_col, _ = table_config
        self.log.info("Saving results to {}".format(table_name))
        db = self._get_db()

        # table_checks
        self.check_tables(table_name)

        table = db.load_table(table_name)

        self.check_columns(table, data)

        inserted_ids = []
        for item in data:
            inserted_id = table.upsert(item, [parent_key_col])
            inserted_ids.append(inserted_id)

            logger.debug('Saved result with id {} for parent {} {}'.format(
                inserted_id,
                parent_key_col,
                item[parent_key_col]))

        return inserted_ids


class OCDSLoader(SQLDBLoader):

    DATA_COLLECTION_TABLE = 'data_collection_logs'

    def get_last_item_date(self):
        sql_query = """
            select max(data.data->>'date') as last_item_date from public.data as data
            where exists(select data_id from public.release where collection_id=34 and release.data_id=data.id);
        """

        results = self.table_from_sql(sql_query)
        last_item_date = results.iloc[0].last_item_date

        return last_item_date

    def log_data_collection(self, data_collection_record):
        self.log.info("Saving results to {}".format(OCDSLoader.DATA_COLLECTION_TABLE))
        db = self._get_db()

        # table_checks
        self.check_tables(OCDSLoader.DATA_COLLECTION_TABLE)

        table = db.load_table(OCDSLoader.DATA_COLLECTION_TABLE)

        data = pd.DataFrame([data_collection_record])
        self.check_columns(table, data)

        inserted_id = table.insert(data_collection_record)
        return inserted_id

    def get_last_data_loading_records(self, n_records):
        sql_query = """
            select * from data_collection_logs
            order by last_item_date desc, timestamp desc
            limit :n_records;
        """

        results = self.table_from_sql(sql_query, params={'n_records': n_records})
        return results


@resource(config_schema={
    'conn_str': Field(str),
    'statement_timeout': Field(int, default_value=SQLDBLoader.DEFAULT_STATEMENT_TIMEOUT, is_required=False)
})
def ocds_loader_resource(context):
    return OCDSLoader(context, context.resource_config['conn_str'], context.resource_config['statement_timeout'])