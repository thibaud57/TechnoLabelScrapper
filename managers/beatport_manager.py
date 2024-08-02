from collections import Counter
from datetime import datetime, timedelta

from constants import ACTIF_MINIMUM_RELEASES_NUMBER, NON, ARTISTS_MINIMUM_NUMBER, OUI
from enums import TypeLink
from loggers import AppLogger
from scrappers import RequestsHelper


class BeatportManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.helper = RequestsHelper()

    def get_beatport_info(self, url, label_name):
        release_url = self._generate_release_url(url)
        data = self.helper.search_label(release_url, TypeLink.BEATPORT_URL)
        if release_info := self._get_last_releases_info(data):
            releases_number = release_info.get('releases_number', 0)
            artists_number = release_info.get('artists_number', 0)
            return {'name': label_name,
                    'actif': OUI if releases_number > ACTIF_MINIMUM_RELEASES_NUMBER else NON,
                    'ouvert_nouveaux': OUI if artists_number > ARTISTS_MINIMUM_NUMBER else NON}
        return None

    @staticmethod
    def get_date_range():
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    def _generate_release_url(self, url):
        start_date, end_date = self.get_date_range()
        release_url = f'{url}/releases?publish_date={start_date}%3A{end_date}&page=1&per_page=100'
        return release_url

    def _get_last_releases_info(self, data):
        if data is None:
            return {'releases_number': 0, 'artists_number': 0, 'artists': None}
        if not isinstance(data, dict):
            raise TypeError('Data must be a dict.')
        releases = data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [{}])[1].get(
            'state',
            {}).get(
            'data', {}).get('results', [])
        if releases:
            artists = self._count_unique_artists(releases)
            return {'releases_number': len(releases), 'artists_number': len(artists), 'artists': artists}
        else:
            return {'releases_number': 0, 'artists_number': 0, 'artists': None}

    def _count_unique_artists(self, releases):
        artists = []
        for release in releases:
            artists.extend([artist['name'] for artist in release.get('artists', [])])
        return Counter(artists)
