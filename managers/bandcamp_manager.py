import re
from urllib.parse import urlparse

from enums import TypeLink
from loggers import AppLogger
from scrappers import RequestsHelper
from utils import CountryExtractor


class BandcampManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.helper = RequestsHelper()

    def get_bandcamp_info(self, label_name):
        search_url = f'{TypeLink.BANDCAMP_URL.value}/search?q={label_name.replace(" ", "+")}s&item_type=b&from=results'
        data = self.helper.scrap_with_requests(search_url, TypeLink.BANDCAMP_URL)
        parsed_results = []
        if not data:
            return None
        for info in data:
            genre_div = info.find(class_='genre')
            if genre_div:
                genre = genre_div.text.strip().split(': ')[-1]
                if genre.lower() != 'electronic':
                    continue
            heading = info.find(class_='heading')
            itemurl = info.find(class_='itemurl')
            if heading and itemurl:
                label_name = heading.a.text.strip() if heading.a else ''
                href = itemurl.a.get('href') if itemurl.a else ''
                if href:
                    parsed_url = urlparse(href)
                    link = f'{parsed_url.scheme}://{parsed_url.netloc}'
                    if not link.endswith('.com'):
                        link = re.sub(r'\?.*', '', href)
                    subhead = info.find(class_='subhead')
                    if subhead:
                        country_extractor = CountryExtractor()
                        country = country_extractor.get_country_name(subhead.text)
                    else:
                        country = None
                    parsed_results.append({
                        'name': label_name,
                        TypeLink.BANDCAMP_URL.name: link,
                        'country': country
                    })
        return parsed_results
