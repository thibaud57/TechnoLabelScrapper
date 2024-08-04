import threading
from concurrent.futures import ThreadPoolExecutor

from constants import THREADS_NUMBER, CREDENTIALS_FILE, SPREADSHEET_ID, OUI
from enums import BeatstatsGenre, TypeLink
from loggers import AppLogger
from managers import BeatstatsManager, GoogleSheetsManager
from utils.utils import find_best_match


class TopProcessor:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.sheets_manager = GoogleSheetsManager(CREDENTIALS_FILE, SPREADSHEET_ID)
        self.beatstats_manager = BeatstatsManager()
        self.genres_lock = threading.Lock()
        self.labels_from_sheet = []
        self.genres_in_success = []
        self.genres_in_failure = []
        self.last_row = 0

    def run(self):
        self.labels_from_sheet = self.sheets_manager.read_columns('Labels!A2:A,R2:R,V2:V')
        self.last_row = len(self.labels_from_sheet) + 1

        with ThreadPoolExecutor(max_workers=THREADS_NUMBER) as executor:
            executor.map(self._process_top_100, [genre for genre in BeatstatsGenre])

        if self.genres_in_success:
            for success_info in self.genres_in_success:
                sheet_labels = self._extract_labels_name_and_beatport_link_from_sheet()
                filter_labels = self._filter_beatstats_labels(sheet_labels, success_info)
                if filter_labels:
                    updates = self.sheets_manager.prepare_batch_updates_for_beatstats(filter_labels)
                    success = self.sheets_manager.batch_update_in_chunks(updates)
                    if not success:
                        self.logger.error('Failed to perform batch update')
                else:
                    genre = success_info['genre']
                    self.logger.warning(f'No new label to add for {genre}')

    def _process_top_100(self, genre: BeatstatsGenre):
        self.logger.info(f'Processing top 100 from Beatstats for {genre.name}')
        try:
            beatstats_labels = self.beatstats_manager.get_top_100_by_genre(genre.value)
            with self.genres_lock:
                self.genres_in_success.append({'genre': genre.name, 'labels': beatstats_labels})
        except Exception as e:
            self.logger.error(f'Error processing top 100 for {genre.name}: {str(e)}')
            with self.genres_lock:
                self.genres_in_failure.append({'genre': genre.name, 'reason': f'Error processing top 100: {str(e)}'})

    def _extract_labels_name_and_beatport_link_from_sheet(self):
        return [
            {'row': row[0], "name": row[1], TypeLink.BEATPORT_URL.name: row[2], 'beatstats_flag': row[3] == OUI}
            for row in self.labels_from_sheet
        ]

    def _filter_beatstats_labels(self, sheet_labels, success_info):
        sheet_urls = {label[TypeLink.BEATPORT_URL.name]: label for label in sheet_labels if
                      TypeLink.BEATPORT_URL.name in label}
        beatstats_labels = success_info['labels']
        updated_labels = []
        for label in beatstats_labels:
            updated_label = None
            if TypeLink.BEATPORT_URL.name in label and label[TypeLink.BEATPORT_URL.name] in sheet_urls:
                updated_label = sheet_urls[label[TypeLink.BEATPORT_URL.name]].copy()
            elif 'name' in label:
                best_match = find_best_match(label['name'], sheet_labels, 99)
                if best_match:
                    updated_label = best_match.copy()
            if updated_label:
                updated_label.update({k: v for k, v in label.items() if k not in updated_label})
            else:
                updated_label = label.copy()
                self.last_row += 1
                updated_label['row'] = self.last_row
                updated_label['is_new'] = True
            updated_labels.append(updated_label)
        return [label for label in updated_labels if not label.get('beatstats_flag', False)]
