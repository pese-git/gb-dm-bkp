import scrapy
import pymongo
import re
from urllib.parse import urljoin, unquote
from base64 import b64decode


class AutoyoulaSpider(scrapy.Spider):
    name = "autoyoula"
    allowed_domains = ["auto.youla.ru"]
    start_urls = ["https://auto.youla.ru/"]

    _re_pattern_dealer_check = re.compile(r"sellerLink%22%2Cnull%2C%22type")
    _re_pattern_user = re.compile(r"youlaId%22%2C%22([a-zA-Z|\d]+)%22%2C%22avatar")
    _re_pattern_dealer = re.compile(r"sellerLink%22%2C%22([\W|a-zA-Z|\d]+)%22%2C%22type")

    _re_pattern_img = re.compile(
        r"%2Fstatic.am%2Fautomobile_m3%2Fdocument%2F([a-zA-z|\d|\%]+).jpg"
    )

    _re_pattern_phone = re.compile(r"phone%22%2C%22([a-zA-Z|\d|%]+)%22%2C%22time")

    _base_image_url = "https://static.am/automobile_m3/document/"

    def __init__(self):
        db_client = pymongo.MongoClient("mongodb://localhost:27017")
        self.db = db_client["gb_data_mining_auto_youla_21_04_2021"]

    def _get_follow(self, response, selector_str, callback):
        for itm in response.css(selector_str):
            url = itm.attrib["href"]
            yield response.follow(url, callback=callback)

    def parse(self, response, *args, **kwargs):
        yield from self._get_follow(
            response,
            ".TransportMainFilters_brandsList__2tIkv .ColumnItemList_container__5gTrc .ColumnItemList_item__32nYI a.blackLink",
            self._brand_parse,
        )

    def _brand_parse(self, response):
        yield from self._get_follow(
            response, ".Paginator_block__2XAPy .Paginator_button__u1e7D", self._brand_parse
        )

        yield from self._get_follow(
            response,
            "article.SerpSnippet_snippet__3O1t2 a.SerpSnippet_name__3F7Yu.blackLink",
            self._car_parse,
        )

    def _car_parse(self, response):
        characteristics = response.css(
            "div.AdvertCard_specs__2FEHc .AdvertSpecs_row__ljPcX *::text"
        ).getall()
        car_dict = {
            "title": response.css(".AdvertCard_advertTitle__1S1Ak::text").get(),
            "price": float(
                response.css("div.AdvertCard_price__3dDCr::text").get().replace("\u2009", "")
            ),
            "characteristics": {
                characteristics[i]: characteristics[i + 1]
                for i in range(0, len(characteristics), 2)
            },
            "description": response.css(
                "div.AdvertCard_descriptionWrap__17EU3 .AdvertCard_descriptionInner__KnuRi::text"
            ).get(),
        }
        car_dict.update(self._get_owner_info(response))
        self._save(car_dict)

    def _get_owner_info(self, response):
        marker = "window.transitState = decodeURIComponent"
        for script in response.css("script"):
            try:
                if marker in script.css("::text").extract_first():
                    advert_dict_update = {
                        "owner_url": self._get_owner_url(response, script),
                        "image_urls": self._get_image(script),
                        "phone_number": self._get_phone_number(script),
                    }
                    return advert_dict_update
            except TypeError:
                pass

    def _get_owner_url(self, response, script):
        if re.findall(self._re_pattern_dealer_check, script.css("::text").extract_first()):
            owner_id = re.findall(self._re_pattern_user, script.css("::text").extract_first())
            return response.urljoin(f"/user/{owner_id[0]}")
        else:
            owner_id = re.findall(self._re_pattern_dealer, script.css("::text").extract_first())
            return urljoin(self.start_urls[0], owner_id[0].replace("%2F", "/"))

    def _get_image(self, script) -> list:
        img_list = re.findall(self._re_pattern_img, script.css("::text").extract_first())
        return [
            urljoin(self._base_image_url, url.replace("%2F", "/") + ".jpg") for url in img_list
        ]

    def _get_phone_number(self, script):
        phone = unquote(
            re.findall(self._re_pattern_phone, script.css("::text").extract_first())[0]
        )
        return b64decode(b64decode(phone)).decode()

    def _save(self, data):
        collection = self.db["cars"]
        collection.insert_one(data)
