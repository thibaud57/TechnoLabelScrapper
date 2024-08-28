import json
import random
import re
import time

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from constants import MAX_RETRIES, BEATPORT_SCRIPT_ID, SOUNDCLOUD_SCRIPT_ID
from enums import StatusCode, TypeLink
from loggers import AppLogger


class RequestsHelper:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.session = requests.Session()
        self.ua = UserAgent()
        self.headers = {'User-Agent': self.ua.random}

    def scrap_with_requests(self, url, type_link):
        backoff_time = 5
        for _ in range(MAX_RETRIES):
            try:
                self.headers['User-Agent'] = self.ua.random
                response = self.session.get(url, headers=self.headers, timeout=10)
                self.logger.info(f'Scrap url: {url} with status: {response.status_code}')
                if response.status_code == StatusCode.SUCCESS.value:
                    return self._process_response(response, type_link)
                elif response.status_code == StatusCode.TOO_MANY_REQUESTS.value:
                    self.logger.warning(f'Received a 429 status code. Retrying in {backoff_time} seconds...')
                    # time.sleep(int(response.headers["Retry-After"]))
                    time.sleep(backoff_time)
                    backoff_time *= 2
                elif response.status_code == StatusCode.FORBIDDEN.value:
                    self.logger.warning('Received a 403 status code. Retrying...')
                    time.sleep(random.uniform(1, 3))
                    continue
                else:
                    self.logger.warning(f'Failed to fetch the page. Status code: {response.status_code}')
                    return None
            except requests.RequestException as e:
                self.logger.error(f'Request error: {e}')
                time.sleep(backoff_time)
                backoff_time *= 2
                continue
            finally:
                self._close()
        self.logger.warning('Max retries reached. Exiting.')
        return None

    def _process_response(self, response, type_link):
        match type_link:
            case TypeLink.BEATPORT_URL:
                return self._beatport_scrapper(response.content)
            case TypeLink.SOUNDCLOUD_URL:
                return self._soundcloud_scrapper(response.content)
            case TypeLink.BEATSTATS_URL:
                return self._beatstats_scrapper(response.content)
            case TypeLink.BANDCAMP_URL:
                return self._bandcamp_scrapper(response.content)

    def _beatport_scrapper(self, content):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            script = soup.find('script', {'id': BEATPORT_SCRIPT_ID})
            if not script:
                self.logger.warning(f'Script with ID {BEATPORT_SCRIPT_ID} not found')
                return None
            try:
                return json.loads(script.string)
            except json.JSONDecodeError:
                self.logger.error('Error while loading json.')
                return None
        except Exception as e:
            self.logger.error(f'Error while scrapping content: {e}')
            return None

    def _soundcloud_scrapper(self, content):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            script = soup.find('script', text=lambda t: t and SOUNDCLOUD_SCRIPT_ID in t)
            if not script:
                self.logger.warning(f'Script with {SOUNDCLOUD_SCRIPT_ID} not found')
                return None
            script_text = script.string
            match = re.search(f'{SOUNDCLOUD_SCRIPT_ID}\s*=\s*(\[[\s\S]*?\]);', script_text)
            if not match:
                self.logger.warning('JSON data not found in the script')
                return None
            json_text = match.group(1)
            try:
                return json.loads(json_text)
            except json.JSONDecodeError:
                self.logger.error('Error while loading JSON.')
                return None
        except Exception as e:
            self.logger.error(f'Error while scrapping content: {e}')
            return None

    def _beatstats_scrapper(self, content):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            content_artists = soup.find(id="content-artists")
            return [] if not content_artists else content_artists
        except Exception as e:
            self.logger.error(f'Error while scrapping content: {e}')
            return None

    def _bandcamp_scrapper(self, content):
        try:
            soup = BeautifulSoup(content, 'html.parser')
            result_infos = soup.find_all(class_="result-info")
            return [] if not result_infos else result_infos
        except Exception as e:
            self.logger.error(f'Error while scrapping content: {e}')
            return None

    def _close(self):
        self.session.close()
