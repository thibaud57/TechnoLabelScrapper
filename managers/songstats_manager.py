import json
from typing import Optional, List, Dict, Any

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from constants import SONGSTATS_URL, SONGSTATS_API_URL, \
    MAX_RETRIES
from enums import TypeLink
from loggers import AppLogger
from scrappers import PlaywrightCrawler
from scrappers.playwright_crawler import RequestInterceptor


class SongstatsManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.crawler = PlaywrightCrawler()

    def get_matching_labels(self, label_name: str) -> List[Dict[str, str]]:
        interceptor = RequestInterceptor(SONGSTATS_API_URL)
        with sync_playwright() as p:
            browser, context, page = self.crawler.init_playwright_config(p)
            page.on('request', interceptor)
            try:
                page.goto(SONGSTATS_URL)
                search_input = page.wait_for_selector('#artistLabelSearchBarInput', state='visible')
                search_input.fill(label_name)
                page.wait_for_timeout(300)
                for request in interceptor.requests:
                    response = request.response()
                    if response:
                        return self.filter_songstats_labels(json.loads(response.text()))
                return []
            except Exception as e:
                self.logger.error(f'An error occurred: {e}')
                return []
            finally:
                self.crawler.close_connection(browser, context)

    def get_label_info(self, label_name: str, label_info: Dict[str, str]) -> Dict[str, str | List[
        Dict[str, str]]] | None:
        label_url = self.build_songstats_url(label_info)
        with sync_playwright() as p:
            browser, context, page = self.crawler.init_playwright_config(p)
            try:
                return self._perform_scraping_with_label_url(page, label_url, label_name)
            except Exception as e:
                self.logger.error(f'An error occurred: {e}')
                return None
            finally:
                self.crawler.close_connection(browser, context)

    def _perform_scraping_with_label_url(self, page: Page, label_url: str, label_name: str) -> Dict[str, Any]:
        page.goto(label_url)
        return {
            'name': label_name,
            'country': self._scrap_label_country(page, label_name),
            'url': label_url,
            'links': self._scrap_label_links(page, label_name)
        }

    def _scrap_label_country(self, page: Page, label_name: str) -> str:
        parent_selector = 'div[style*="display: flex; flex-direction: column; align-items: center;"]'
        country_span_selector = f'{parent_selector} > div:last-child > span'

        for attempt in range(MAX_RETRIES):
            try:
                timeout = 600 * (2 ** attempt)
                page.wait_for_selector(country_span_selector, state='visible', timeout=timeout)
                country = page.query_selector(country_span_selector)
                if country:
                    return country.inner_text()
            except PlaywrightTimeoutError:
                self.logger.info(f'country not found on attempt {attempt + 1}/{MAX_RETRIES} for {label_name}')
            except Exception as e:
                self.logger.error(f'An error occurred: {e}')
        return ''

    def _scrap_label_links(self, page: Page, label_name: str) -> Dict[str, str]:
        label_links = {}
        for type_link in TypeLink:
            link = self._get_link(page, type_link.value, label_name)
            if link:
                label_links[type_link.value] = link
        return label_links

    def _get_link(self, page: Page, url: str, label_name: str) -> Optional[str]:
        selector = f'a[href*="{url}"]'
        for attempt in range(MAX_RETRIES):
            try:
                timeout = 600 * (2 ** attempt)
                page.wait_for_selector(selector, state='visible', timeout=timeout)
                link = page.query_selector(selector)
                if link:
                    return link.get_attribute('href')
            except PlaywrightTimeoutError:
                self.logger.info(f'{url} not found on attempt {attempt + 1}/{MAX_RETRIES} for {label_name}')
            except Exception as e:
                self.logger.error(f'An error occurred: {e}')
        return None

    @staticmethod
    def filter_songstats_labels(data: Dict[str, Any]) -> List[Dict[str, str]]:
        return [item for item in data['results'] if item['type'] == 'label']

    @staticmethod
    def build_songstats_url(label: Dict[str, str]) -> str:
        return f"{SONGSTATS_URL}{label['routeInfo']['url']}"
