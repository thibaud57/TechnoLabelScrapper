import math
import threading
from concurrent.futures import ThreadPoolExecutor

from constants import THREADS_NUMBER, CREDENTIALS_FILE, SPREADSHEET_ID, OUI
from enums import BeatstatsGenre, TypeLink
from loggers import AppLogger
from managers import BeatstatsManager, GoogleSheetsManager
from utils.utils import find_best_match, extract_number


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
        self.is_hype = False

    def run(self):
        with ThreadPoolExecutor(max_workers=THREADS_NUMBER) as executor:
            executor.map(self._process_top_100, [genre for genre in BeatstatsGenre])

        if self.genres_in_success:
            for success_info in self.genres_in_success:
                self._refresh_sheet_data(success_info['genre'])
                sheet_labels = self._extract_labels_name_and_beatport_link_from_sheet()
                filter_labels = self._filter_beatstats_labels(sheet_labels, success_info)
                if filter_labels:
                    updates = self.sheets_manager.prepare_batch_updates_for_beatstats(filter_labels)
                    success = self.sheets_manager.batch_update_in_chunks(updates)
                    if not success:
                        self.logger.error('Failed to perform batch update')
                else:
                    self.logger.info(f'No updates to perform for genre {success_info["genre"]}')
        else:
            self.logger.info('No updates to perform for any genre')

    def _refresh_sheet_data(self, genre:BeatstatsGenre):
        self.labels_from_sheet = self.sheets_manager.read_columns('Labels!A2:A,C2:C,T2:T,R2:R,V2:V')
        self.last_row = len(self.labels_from_sheet) + 1
        self.is_hype = genre in [
            BeatstatsGenre.HYPE_TECHNO_PEAK_TIME.name,
            BeatstatsGenre.HYPE_MELODIC_HOUSE_TECHNO.name,
        ]

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
            {'row': row[0],
             'name': row[1],
             'genre': row[2],
             'position': row[3],
             TypeLink.BEATPORT_URL.name: row[4],
             'beatstats_flag': row[5] == OUI}
            for row in self.labels_from_sheet
        ]

    def _filter_beatstats_labels(self, sheet_labels, success_info):
        sheet_urls = {label[TypeLink.BEATPORT_URL.name]: label for label in sheet_labels if
                      TypeLink.BEATPORT_URL.name in label}
        beatstats_labels = success_info['labels']
        updated_labels = []

        for label in beatstats_labels:
            sheet_label = None

            if TypeLink.BEATPORT_URL.name in label and label[TypeLink.BEATPORT_URL.name] in sheet_urls:
                sheet_label = sheet_urls[label[TypeLink.BEATPORT_URL.name]].copy()
            elif 'name' in label:
                best_match = find_best_match(label['name'], sheet_labels, 99)
                if best_match:
                    sheet_label = best_match.copy()

            if sheet_label:
                updated_label = self._update_label_with_position_and_genre(sheet_label, label)
                updated_labels.append(updated_label)
            else:
                new_label = label.copy()
                self.last_row += 1
                new_label['row'] = self.last_row
                new_label['position'] = self._format_position_with_hype(label.get('position', math.inf))
                updated_labels.append(new_label)

        return [label for label in updated_labels if not label.get('beatstats_flag', False)]

    def _update_label_with_position_and_genre(self, sheet_label, new_label):
        current_position = sheet_label.get('position', math.inf)
        current_position_number = extract_number(current_position)
        new_position = self._format_position_with_hype(new_label.get('position', math.inf))
        new_position_number = extract_number(new_position)
        if not current_position or new_position_number < current_position_number:
            sheet_label['genre'] = new_label.get('genre', '')
            sheet_label['position'] = new_position
            sheet_label['beatstats_flag'] = False
            sheet_label['update_label'] = True
            sheet_label.update({k: v for k, v in sheet_label.items()})
        return sheet_label

    def _format_position_with_hype(self, position):
        return f"{position} HYPE" if self.is_hype else position
