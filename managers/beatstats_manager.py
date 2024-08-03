from constants import BEATSTATS_LIST_GENRE_URL
from enums import TypeLink, BeatstatsGenre
from enums.music_genre import MusicGenre
from loggers import AppLogger
from scrappers import RequestsHelper
from utils.utils import format_title_case


class BeatstatsManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.helper = RequestsHelper()

    def get_top_100_by_genre(self, code_genre):
        try:
            url = f'{BEATSTATS_LIST_GENRE_URL}{code_genre}'
            data = self.helper.scrap_with_requests(url, TypeLink.BEATSTATS_URL)
            names = self._extract_label_names(data)
            links = self._extract_beatport_links(data)
            positions = self._extract_beatstats_positions(data)
            label_data = [
                {
                    "name": name,
                    "genre": self._map_beatstats_genre_to_music_genre(code_genre),
                    TypeLink.BEATPORT_URL.name: f'https://www.{link}',
                    "position": position,
                    "is_hype": self._is_genre_hype(code_genre)
                }
                for name, link, position in zip(names, links, positions)
            ]
            return [] if not label_data else label_data
        except Exception as e:
            self.logger.error(f'Error getting Beatstats top 100 for {code_genre}: {str(e)}')
            return None

    def _extract_label_names(self, data):
        name_spans = data.find_all('span', class_='labelcharttextname')
        label_names = [format_title_case(span.text.strip()) for span in name_spans]
        return label_names

    def _extract_beatport_links(self, data):
        links = data.find_all('a')
        beatport_links = [f"{TypeLink.BEATPORT_URL.value}{link.get('href')}" for link in links if
                          link.get('href') and link.get('href').startswith('/label')]
        return beatport_links

    def _extract_beatstats_positions(self, data):
        position_divs = data.find_all('div', id='top10artistchart-number')
        return [
            div.contents[0].strip()
            for div in position_divs
            if div.contents
        ]

    def _map_beatstats_genre_to_music_genre(self, genre):
        genre_mapping = {
            BeatstatsGenre.TECHNO_PEAK_TIME.value: MusicGenre.PEAK_TIME.value,
            BeatstatsGenre.HYPE_TECHNO_PEAK_TIME.value: MusicGenre.PEAK_TIME.value,
            BeatstatsGenre.TECHNO_RAW_DEEP_HYPNOTIC.value: MusicGenre.TECHNO.value,
            BeatstatsGenre.TRANCE_RAW_DEEP_HYPNOTIC.value: MusicGenre.TRANCE.value,
            BeatstatsGenre.PROGRESSIVE_HOUSE.value: MusicGenre.PROGRESSIVE.value,
            BeatstatsGenre.MELODIC_HOUSE_TECHNO.value: MusicGenre.MELODIC.value,
            BeatstatsGenre.HYPE_MELODIC_HOUSE_TECHNO.value: MusicGenre.MELODIC.value
        }
        if genre in genre_mapping:
            return genre_mapping[genre]
        else:
            return genre

    def _is_genre_hype(self, genre):
        hype = [
            BeatstatsGenre.HYPE_TECHNO_PEAK_TIME.value,
            BeatstatsGenre.HYPE_MELODIC_HOUSE_TECHNO.value,
        ]
        return genre in hype
