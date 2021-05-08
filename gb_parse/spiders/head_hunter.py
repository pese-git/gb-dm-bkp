import scrapy
from ..loaders import HeadHunterLoader


class HeadhunterSpider(scrapy.Spider):
    name = "headhunter"
    allowed_domains = ["hh.ru"]
    start_urls = ["https://hh.ru/search/vacancy?schedule=remote&L_profession_id=0&area=113"]

    _hh_xpaths = {
        "pagination": "//div[contains(@class, 'vacancy-serp-wrapper')]//div[@data-qa='pager-block']"
        "//a[@data-qa='pager-page']",
        "vacancy": "//div[contains(@class, 'vacancy-serp-item')]//div[@class='vacancy-serp-item__info']"
        "//a[@data-qa='vacancy-serp__vacancy-title']",
        "company_vacancies": "//div[@class='employer-sidebar']//a[contains(@data-qa, employer-vacancies-link)]",
    }

    _vacancy_xpaths = {
        "title": "//div[@class='bloko-columns-wrapper']//h1[@data-qa='vacancy-title']//text()",
        "salary": "//div[@class='bloko-columns-wrapper']//p[@class='vacancy-salary']/span/text()",
        "description": "//div[contains(@class, 'bloko-gap_bottom')]//div[@class='vacancy-description']//text()",
        "core_skills": "//div[@class='vacancy-section']//div[@class='bloko-tag-list']//text()",
        "hh_employer_url": "//div[@class='vacancy-company-wrapper']//a[@data-qa='vacancy-company-name']",
    }

    _employer_xpaths = {
        "employer_name": "//div[@class='company-header']//span[@data-qa='company-header-title-name']/text()",
        "employer_website": "//div[@class='employer-sidebar']//a[@data-qa='sidebar-company-site']/@href",
        "area_of_activity": "//div[@class='employer-sidebar']//div[@class='employer-sidebar-block']"
        "/p/text()",
        "description_employer": "//div[contains(@class, 'bloko-gap')]"
        "//div[@data-qa='company-description-text']//text()",
    }

    def _get_follow_xpath(self, response, select_str, callback):
        for a in response.xpath(select_str):
            link = a.attrib.get("href")
            yield response.follow(link, callback=callback)

    def _get_loader_xpaths(self, loader_name, response, xpaths, **kwargs):
        loader = loader_name(response=response)
        for key, selector in xpaths.items():
            loader.add_xpath(key, selector)
        if kwargs:
            for k, v in kwargs.items():
                loader.add_value(k, v)
        yield loader.load_item()

    def parse(self, response, *args, **kwargs):
        yield from self._get_follow_xpath(response, self._hh_xpaths["pagination"], self.parse)

        yield from self._get_follow_xpath(
            response, self._hh_xpaths["vacancy"], self._parse_vacancies
        )

    def _parse_vacancies(self, response):
        yield from self._get_loader_xpaths(HeadHunterLoader, response, self._vacancy_xpaths)
        yield from self._get_follow_xpath(
            response, self._vacancy_xpaths["hh_employer_url"], self._parse_employer
        )

    def _parse_employer(self, response):
        yield from self._get_loader_xpaths(
            HeadHunterLoader, response, self._employer_xpaths, employer_link_hh=response.url
        )
        yield from self._get_follow_xpath(
            response, self._hh_xpaths["company_vacancies"], self.parse
        )
