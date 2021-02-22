from pathlib import Path
import timer
import json
import requests


class Parse5ka:
    def __init__(self, start_url: str, products_path: Path):
        self._start_url = start_url
        self._products_path = products_path
        self._headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/88.0.4324.182 Safari/537.36"
        }

    def _get_response(self, url: str):
        while True:
            response = requests.get(url, params=None, headers=self._headers)
            if response.status_code == 200:
                return response
            timer.sleep(0.5)

    def run(self):
        for product in self._parse(self._start_url):
            file_name = f"{[product['id']]}.json"
            product_path = self._products_path.joinpath(file_name)
            self._save(product, product_path)

    def _parse(self, full_url: str):
        response = None
        try:
            while full_url:
                response = self._get_response(full_url)
                data = response.json()
                full_url = data["next"]
                for product in data["results"]:
                    yield product

        except Exception:
            if response is not None:
                print(response.text)
            print(Exception)

    @staticmethod
    def _save(data: dict, file_path):
        dump = json.dumps(data, ensure_ascii=False)
        file_path.write_text(dump, encoding="UTF-8")


class CategoriesParser(Parse5ka):
    def __init__(self, categories_url: str, *args, **kwargs):
        self._categories_url = categories_url
        super().__init__(*args, **kwargs)

    def _get_categories(self):
        response = self._get_response(self._categories_url)
        data = response.json()
        return data

    def run(self):
        for category in self._get_categories():
            category["products"] = []
            code = category["parent_group_code"]
            curr_url = f"{self._start_url}?categories={code}"
            file_path = self._products_path.joinpath(f"{code}.json")
            category["products"].extend(list(self._parse(curr_url)))
            self._save(category, file_path)


def get_dir_path(dirname: str) -> Path:
    dir_path = Path(__file__).parent.joinpath(dirname)
    if not dir_path.exists():
        dir_path.mkdir()
    return dir_path


if __name__ == "__main__":
    url = "https://5ka.ru/api/v2/special_offers/"
    cat_url = "https://5ka.ru/api/v2/categories/"

    products_path = get_dir_path("products")
    cat_path = get_dir_path("categories")

    # parser = Parse5ka(url, products_path)
    cat_parser = CategoriesParser(cat_url, url, cat_path)
    cat_parser.run()
