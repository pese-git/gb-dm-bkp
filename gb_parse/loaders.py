from scrapy import Selector
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose

import re
from urllib.parse import urljoin


def clear_price(price):
    try:
        result = float(price.replace("\u2009", ""))
    except ValueError:
        result = None
    return result


def get_characteristics(item: str) -> dict:
    selector = Selector(text=item)
    data = {}
    data["name"] = selector.xpath(
        "//div[contains(@class, 'AdvertSpecs_label')]/text()"
    ).extract_first()
    data["value"] = selector.xpath(
        "//div[contains(@class, 'AdvertSpecs_data')]//text()"
    ).extract_first()
    return data


def get_author_id(text):
    re_pattern = re.compile(r"youlaId%22%2C%22([a-zA-Z|\d]+)%22%2C%22avatar")
    result = re.findall(re_pattern, text)
    try:
        user_link = f"https://youla.ru/user/{result[0]}"
    except IndexError:
        user_link = None
        pass
    return user_link


class AutoyoulaLoader(ItemLoader):
    default_item_class = dict
    url_out = TakeFirst()
    title_out = TakeFirst()
    price_in = MapCompose(clear_price)
    price_out = TakeFirst()
    description_out = TakeFirst()
    characteristics_in = MapCompose(get_characteristics)
    author_in = MapCompose(get_author_id)
    author_out = TakeFirst()


# -------------------


def create_author_url(item):
    selector = Selector(text=item)
    url = selector.xpath("//@href").get()
    return urljoin("https://hh.ru/", url)


def create_text(item):
    return "".join(item)


class HeadHunterLoader(ItemLoader):
    default_item_class = dict
    title_out = TakeFirst()
    hh_employer_url_in = MapCompose(create_author_url)
    hh_employer_url_out = TakeFirst()
    salary_out = create_text
    description_out = create_text

    employer_name_out = TakeFirst()
    employer_website_out = TakeFirst()
    description_employer_out = create_text
    employer_link_hh_out = TakeFirst()
