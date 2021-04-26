import requests
import typing
from urllib.parse import urljoin
import bs4

import pymongo

from datetime import datetime


class GbBlogParse:
    def __init__(self, start_url, db_client):
        self.db = db_client["gb_data_mining_20_04_2021"]
        self.start_url = start_url
        self.done_urls = set()
        self.tasks = []

    def _get_response(self, url):
        response = requests.get(url)
        return response

    def _get_soup(self, url):
        response = self._get_response(url)
        soup = bs4.BeautifulSoup(response.text, "lxml")
        return soup

    def __create_set_links(self, url, tag_list):
        result = set()
        for href in tag_list:
            if href.attrs.get("href"):
                result.add(urljoin(url, href.attrs.get("href")))

        return result

    def __create_task(self, url, callback, tag_list):
        for link in self.__create_set_links(url, tag_list):
            if link not in self.done_urls:
                task = self._get_task(link, callback)
                self.tasks.append(task)

    def _parse_date(self, date_str):

        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")

    def _parse_feed(self, url, soup) -> None:
        ul = soup.find("ul", attrs={"class": "gb__pagination"})
        self.__create_task(url, self._parse_feed, ul.find_all("a"))
        a_tags = soup.find_all("a", attrs={"class": "post-item__title"})
        self.__create_task(url, self._parse_post, a_tags)

    def _parse_post(self, url, soup) -> dict:
        author_name_tag = soup.find("div", attrs={"itemprop": "author"})
        post_title = soup.find("h1", attrs={"class": "blogpost-title"}).text
        post_date = soup.find("time", attrs={"itemprop": "datePublished"}).attrs.get("datetime")
        post_id = soup.find("comments").attrs.get("commentable-id")
        data = {
            "post_data": {
                "url": url,
                "title": post_title,
                "create_at": self._parse_date(post_date),
                "id": post_id,
            },
            "author": {
                "name": author_name_tag.text,
                "url": urljoin(url, author_name_tag.parent.attrs.get("href")),
            },
            "tags": [
                {"name": tag.text, "url": urljoin(url, tag.attrs.get("href"))}
                for tag in soup.find_all("a", attrs={"class": "small"})
            ],
            "comments_data": self._get_comments(post_id),
        }
        return data

    def _get_comments(self, post_id) -> list:
        # https://geekbrains.ru/api/v2/comments?commentable_type=Post&commentable_id=2541&order=desc
        # https://geekbrains.ru/api/v2/comments?commentable_type=Post&commentable_id=2543&order=desc
        # https://geekbrains.ru/api/v2/comments?commentable_table=Post&commentable_id=$2541&order=desc
        api_path = f"/api/v2/comments?commentable_type=Post&commentable_id={post_id}&order=desc"
        response = self._get_response(self.start_url + api_path)
        data = response.json()
        return data

    def _get_task(self, url, callback: typing.Callable) -> typing.Callable:
        def task():
            soup = self._get_soup(url)
            return callback(url, soup)

        if url in self.done_urls:
            return lambda *_, **__: None
        self.done_urls.add(url)
        return task

    def run(self):
        self.tasks.append(self._get_task(self.start_url + "/posts/", self._parse_feed))
        self.done_urls.add(self.start_url + "/posts/")
        for task in self.tasks:
            result = task()
            if isinstance(result, dict):
                print(result)
                self.save(result)
            print(1)

    def save(self, data):
        collection = self.db["post"]
        collection.insert_one(data)


if __name__ == "__main__":
    db_client = pymongo.MongoClient("mongodb://localhost:27017")
    url = "https://geekbrains.ru"
    parser = GbBlogParse(url, db_client)

    parser.run()
