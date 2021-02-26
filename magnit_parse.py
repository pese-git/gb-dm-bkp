import requests
import bs4
from urllib.parse import urljoin
import pymongo
from datetime import datetime


class MagnitParse:
    def __init__(self, start_url, db_client):
        self.start_url = start_url
        self.db = db_client["gb_data_mining_16_02_2021"]

    def _template(self):
        # "url": str,
        # "promo_name": str,
        # "product_name": str,
        # old_price": float,
        # "new_price": float,
        # "image_url": str,
        # "date_from": "DATETIME",
        # "date_to": "DATETIME",
        return {
            "url": lambda a: urljoin(self.start_url, a.attrs.get("href")),
            "image_url": lambda a: self._get_image_url(a),
            "product_name": lambda a: self._get_product_name(a),
            "promo_name": lambda a: self._get_promo_name(a),
            "old_price": lambda a: self._get_price(a, "label__price_old"),
            "new_price": lambda a: self._get_price(a, "label__price_new"),
            "date_from": lambda a: self._get_date(
                a.find("div", attrs={"class", "card-sale__date"}).text
            )[0],
            "date_to": lambda a: self._get_date(
                a.find("div", attrs={"class", "card-sale__date"}).text
            )[1],
        }

    def _get_image_url(self, a) -> str:
        return urljoin(self.start_url, a.find("img").attrs.get("data-src"))

    def _get_product_name(self, a) -> str:
        return a.find("div", attrs={"class", "card-sale__title"}).text

    def _get_promo_name(self, a) -> str:
        return a.find("div", attrs={"class", "card-sale__name"}).text

    def _get_price(self, a, class_name) -> float:
        text = a.find("div", attrs={"class", class_name}).text
        return float(".".join(item for item in text.split()))

    def _get_date(self, date_str) -> list:
        month_converter = {
            "января": "Jan",
            "февраля": "Feb",
            "марта": "March",
            "апреля": "Apr",
            "мая": "May",
            "июня": "Jun",
            "июля": "Jul",
            "августа": "Aug",
            "сентября": "Sep",
            "октября": "Oct",
            "ноября": "Nov",
            "декабря": "Dec",
        }
        year = datetime.today().year
        date_list = date_str.replace("с ", "", 1).replace("\n", "").split("до")
        result = []
        for date in date_list:
            args = date.strip().split(" ")
            if len(args) == 2:
                build = "%s %s %d" % (args[0], month_converter[args[1]], year)
                result.append(datetime.strptime(build, "%d %b %Y"))
        print(result)
        return result

    def _get_response(self, url):
        # TODO: написать обработку ошибок
        response = requests.get(url)
        return response

    def _get_soup(self, url):
        response = self._get_response(url)
        soup = bs4.BeautifulSoup(response.text, "lxml")
        return soup

    def run(self):
        soup = self._get_soup(self.start_url)
        catalog = soup.find("div", attrs={"class": "сatalogue__main"})

        for product_a in catalog.find_all("a", recursive=False):
            product_data = self._parse(product_a)
            self.save(product_data)

    def _parse(self, product_a: bs4.Tag) -> dict:
        product_data = {}
        for key, func in self._template().items():
            try:
                product_data[key] = func(product_a)
            except AttributeError:
                pass
            except ValueError:
                pass
            except IndexError:
                pass
        return product_data

    def save(self, data: dict):
        collection = self.db["magnit"]
        collection.insert_one(data)
        print(1)


if __name__ == "__main__":
    url = "https://magnit.ru/promo/?geo=moskva"
    db_client = pymongo.MongoClient("mongodb://localhost:27017")
    parse = MagnitParse(url, db_client)
    parse.run()
