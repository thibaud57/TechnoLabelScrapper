import json
import re

import requests
from bs4 import BeautifulSoup

from constants import MAX_RETRIES, USER_AGENTS, BEATPORT_SCRIPT_ID, SOUNDCLOUD_SCRIPT_ID
from enums import StatusCode, TypeLink
from loggers import AppLogger


class RequestsHelper:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.session = requests.Session()
        self.headers = {'User-Agent': USER_AGENTS[0]}

    def search_label(self, url, type_link):
        for _ in range(MAX_RETRIES):
            try:
                response = self.session.get(url, headers=self.headers)
                self.logger.info(f'Scrap url: {url} with status: {response.status_code}')
                if response.status_code == StatusCode.SUCCESS.value:
                    match type_link:
                        case TypeLink.BEATPORT_URL:
                            return self._beatport_crawler(response.content)
                        case TypeLink.SOUNDCLOUD_URL:
                            return self._soundcloud_crawler(response.content)
                elif response.status_code == StatusCode.FORBIDDEN.value:
                    self.logger.warning('Received a 403 status code. Retrying...')
                    continue
                else:
                    self.logger.warning(f'Failed to fetch the page. Status code: {response.status_code}')
                    return None
            except requests.RequestException as e:
                self.logger.error(f'Request error: {e}')
                continue
            finally:
                self._close()
        self.logger.warning('Max retries reached. Exiting.')
        return None

    def _beatport_crawler(self, content):
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
            self.logger.error(f'Error while parsing content: {e}')
            return None

    def _soundcloud_crawler(self, content):
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
            self.logger.error(f'Error while parsing content: {e}')
            return None

    def _close(self):
        self.session.close()
