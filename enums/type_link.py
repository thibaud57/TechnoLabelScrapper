from enum import Enum


class TypeLink(Enum):
    BEATPORT_URL = "beatport.com"
    SOUNDCLOUD_URL = "soundcloud.com"
    FACEBOOK_URL = "facebook.com"
    INSTAGRAM_URL = "instagram.com"
    BEATSTATS_URL = "beatstats.com"
    BANDCAMP_URL = "https://bandcamp.com"

    @classmethod
    def values(cls):
        return [link.value for link in cls]
