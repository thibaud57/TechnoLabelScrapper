from functools import lru_cache

import pycountry
import pycountry_convert as pc
import unicodedata
from fuzzywuzzy import process


class CountryExtractor:
    COUNTRY_ALIASES = {
        'UK': 'GB', 'USA': 'US', 'Russia': 'RU', 'South Korea': 'KR',
        'Bolivia': 'BO'
    }
    KNOWN_CITIES = {
        'New York': 'US', 'London': 'GB', 'Paris': 'FR', 'Tokyo': 'JP',
        'Berlin': 'DE', 'Montreal': 'CA', 'Toronto': 'CA', 'Vancouver': 'CA',
    }
    COUNTRY_SIMPLIFICATIONS = {
        'United States': 'USA',
        'United Kingdom': 'UK',
        'Russian Federation': 'Russia',
        'Korea, Republic of': 'South Korea',
        'Bolivia, Plurinational State of': 'Bolivia'
    }

    def __init__(self):
        self._init_country_data()
        self._init_subdivision_data()

    def _init_country_data(self):
        self.countries = {self._normalize(country.name): country.alpha_2 for country in pycountry.countries}
        self.countries.update({country.alpha_2.lower(): country.alpha_2 for country in pycountry.countries})
        self.countries.update({country.alpha_3.lower(): country.alpha_2 for country in pycountry.countries})
        self.countries.update({self._normalize(k): v for k, v in self.COUNTRY_ALIASES.items()})

    def _init_subdivision_data(self):
        self.subdivisions = {}
        for subdivision in pycountry.subdivisions:
            normalized_name = self._normalize(subdivision.name)
            self.subdivisions[normalized_name] = subdivision.country_code
            if '-' in subdivision.code:
                self.subdivisions[subdivision.code.split('-')[1].lower()] = subdivision.country_code

    @staticmethod
    def _normalize(text):
        return ''.join(c for c in unicodedata.normalize('NFKD', text.lower()) if not unicodedata.combining(c))

    @lru_cache(maxsize=1024)
    def get_country_name(self, value):
        parts = [self._normalize(part.strip()) for part in value.split(',')]
        for part in reversed(parts):
            if part in self.KNOWN_CITIES:
                return self._simplify_country_name(self._get_country_name_from_code(self.KNOWN_CITIES[part]))
            if part in self.countries:
                return self._simplify_country_name(self._get_country_name_from_code(self.countries[part]))
            if part in self.subdivisions:
                return self._simplify_country_name(self._get_country_name_from_code(self.subdivisions[part]))
        best_match = process.extractOne(parts[-1], self.countries.keys(), score_cutoff=90)
        if best_match:
            country_name = self._get_country_name_from_code(self.countries[best_match[0]])
            return self._simplify_country_name(country_name)
        return None

    @lru_cache(maxsize=256)
    def _get_country_name_from_code(self, alpha_2):
        try:
            return pc.country_alpha2_to_country_name(alpha_2)
        except (KeyError, ValueError):
            return pycountry.countries.get(alpha_2=alpha_2).name

    def _simplify_country_name(self, country_name):
        return self.COUNTRY_SIMPLIFICATIONS.get(country_name, country_name)
