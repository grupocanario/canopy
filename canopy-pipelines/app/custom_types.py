from dagster import DagsterType
import pandas as pd

PandasDataFrame = DagsterType(
    name='PandasDataFrame',
    type_check_fn=lambda _, value: isinstance(value, pd.DataFrame),
    description='A pandas dataframe',
)

PandasSeries = DagsterType(
    name='PandasSeries',
    type_check_fn=lambda _, value: isinstance(value, pd.Series),
    description='A pandas series',
)