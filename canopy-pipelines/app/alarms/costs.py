from bs4 import BeautifulSoup
import requests
from dataclasses import dataclass, asdict
from fuzzywuzzy import fuzz, process

from utils import parse_number


@dataclass
class Item:
    """Represents the item in db"""
    code: str
    description: str
    quantity: float
    unit: str
    price: float

    def as_dict(self):
        dictionary = asdict(self)
        return dictionary


class ItemFinderSecop2:

    def parse_single_item(self, html_item) -> Item:
        tds = html_item.find_all('td')
        texts = [td.text.strip() for td in tds]

        return Item(
            code=texts[1],
            description=texts[3],
            quantity=parse_number(texts[4]),
            unit=texts[5],
            price=parse_number(texts[6])
        )

    def find_items(self, url: str) -> [Item]:
        r = requests.get(url)
        data = r.text
        soup = BeautifulSoup(data, "html.parser")

        items_html = soup.select('table.PriceListLineTable')
        items = [self.parse_single_item(item) for item in items_html]

        return items


class ReferenceItemMatcher:
    def __init__(self, items, score_cutoff=80):
        # TODO separate into price and
        self.items = items
        self.score_cutoff = score_cutoff

    def find_match(self, item_description):
        if item_description:
            return process.extractOne(item_description, self.items, scorer=fuzz.partial_ratio,
                                      score_cutoff=self.score_cutoff)
        else:
            return None
