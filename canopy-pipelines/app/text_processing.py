import pandas as pd
from toolz import curry
from toolz.functoolz import pipe


@curry
def remove_pattern(pattern, series: pd.Series):
    return series.str.replace(pattern, '')


class TextPreprocessor:

    def __init__(self):
        self._init_pipeline()

    def _init_pipeline(self):
        # Get rid of URLs
        text_operations = [
            # Get rid of URLs
            remove_pattern('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'),
            # Take out retweet header, there is only one
            remove_pattern('RT @[a-z,A-Z]*: '),
            # Get rid of hashtags
            remove_pattern('#'),
            # Get rid of references to other screennames
            remove_pattern('@[a-z,A-Z]*'),
            # everything thats no a number or space
            remove_pattern('[^\w\s]'),
            lambda series: series.str.replace('\s\s+', ' '),
            # strip series entries
            lambda series: series.str.strip()
        ]

        self.text_pipeline = lambda data: pipe(data, *text_operations)

    def preprocess(self, texts: pd.Series):
        return self.text_pipeline(texts)
