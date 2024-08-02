from enums import TypeLink
from loggers import AppLogger
from managers import MatchManager
from scrappers import RequestsHelper


class SoundcloudManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.helper = RequestsHelper()
        self.match_manager = MatchManager()

    def get_soundcloud_info(self, url, label_name):
        try:
            data = self.helper.search_label(url, TypeLink.SOUNDCLOUD_URL)
            user_profile_info = self._get_user_profile_info(data)
            if user_profile_info:
                return {
                    'name': label_name,
                    'email_demo': self.match_manager.find_demo_email(user_profile_info.get('description', '')),
                    'soundcloud_followers': user_profile_info.get('followers_number', 0)
                }
            return None
        except Exception as e:
            self.logger.error(f'Error getting Soundcloud info for {label_name}: {str(e)}')
            return None

    def _get_user_profile_info(self, data):
        if not isinstance(data, list):
            self.logger.error('Invalid data format: expected list of dictionaries')
            return None
        for item in data:
            if isinstance(item, dict) and item.get('hydratable') == 'user':
                user_data = item.get('data', {})
                return {
                    'description': user_data.get('description', ''),
                    'followers_number': user_data.get('followers_count', 0)
                }
        self.logger.warning('No user data found in the provided data')
        return None
