import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, List

from constants import THREADS_NUMBER, CREDENTIALS_FILE, SPREADSHEET_ID, OUI
from enums import MenuAction, TypeLink
from loggers import AppLogger
from managers import GoogleSheetsManager, SongstatsManager, BeatportManager, SoundcloudManager, BandcampManager
from utils.utils import find_best_match


class LabelProcessor:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.sheets_manager = GoogleSheetsManager(CREDENTIALS_FILE, SPREADSHEET_ID)
        self.filtered_labels_from_sheet: List[Dict[str, Any]] = []
        self.labels_in_success: List[Dict[str, Any]] = []
        self.labels_in_failure: List[Dict[str, str]] = []
        self.labels_info: Dict[int, Dict[str, Any]] = {}
        self.total_labels_to_proceed = 0
        self.labels_lock = threading.Lock()

    def run(self, action: MenuAction):
        self.filtered_labels_from_sheet = self._build_labels_name_from_sheet(
            action == MenuAction.PROCESS_SONGSTATS.value)

        if not self.filtered_labels_from_sheet:
            self.logger.warning('No labels to process. Exiting.')
            return

        self.total_labels_to_proceed = len(self.filtered_labels_from_sheet)
        self.logger.info(f'Total labels to process: {self.total_labels_to_proceed}')

        process_method = self._get_process_method(action)

        with ThreadPoolExecutor(max_workers=THREADS_NUMBER) as executor:
            list(executor.map(process_method, self.filtered_labels_from_sheet))

        if self.labels_in_success:
            updates = self._prepare_batch_for_updates(action)
            success = self.sheets_manager.batch_update_in_chunks(updates)
            if not success:
                self.logger.error('Failed to perform batch update')

    def _build_labels_name_from_sheet(self, is_songstats: bool) -> List[Dict[str, Any]]:
        if is_songstats:
            labels = self.sheets_manager.read_columns('Labels!A2:A,U2:U')
            return [
                {'row': row[0], 'name': row[1]}
                for row in labels
                if row[2] != OUI
            ]
        else:
            labels = self.sheets_manager.read_columns('Labels!A2:A,R2:R,O2:O,P2:P,Q2:Q')
            return [
                {
                    'row': row[0],
                    'name': row[1],
                    TypeLink.BEATPORT_URL.name: row[2] if len(row) > 2 and row[2] else None,
                    TypeLink.SOUNDCLOUD_URL.name: row[3] if len(row) > 3 and row[3] else None,
                    TypeLink.FACEBOOK_URL.name: row[4] if len(row) > 4 and row[4] else None,
                    TypeLink.INSTAGRAM_URL.name: row[5] if len(row) > 5 and row[5] else None,
                }
                for row in labels
                if row[2] or row[3] or row[4] or row[5]
            ]

    def _get_process_method(self, action):
        match action:
            case MenuAction.PROCESS_SONGSTATS.value:
                return self._process_label_content_from_songstats
            case MenuAction.PROCESS_LINKS.value:
                return self._process_label_for_links
            case MenuAction.PROCESS_VINYLS.value:
                return self._process_label_for_vinyls

    def _get_batch_method(self, action):
        match action:
            case MenuAction.PROCESS_SONGSTATS.value:
                return self._process_label_content_from_songstats
            case MenuAction.PROCESS_LINKS.value:
                return self._process_label_for_links
            case MenuAction.PROCESS_VINYLS.value:
                return self._process_label_for_vinyls

    def _prepare_batch_for_updates(self, action):
        match action:
            case MenuAction.PROCESS_SONGSTATS.value:
                return self.sheets_manager.prepare_batch_updates_for_songstats(
                    self.labels_in_success)
            case MenuAction.PROCESS_LINKS.value:
                return self.sheets_manager.prepare_batch_updates_for_links(
                    self.labels_in_success)
            case MenuAction.PROCESS_VINYLS.value:
                return self.sheets_manager.prepare_batch_updates_for_vinyles(self.labels_in_success)

    def _process_label_content_from_songstats(self, label: Dict[str, Any]):
        try:
            label_name, label_row = self._get_label_info(label)
            if not label_row:
                return

            songstats_manager = SongstatsManager()
            labels_info = songstats_manager.get_matching_labels(label_name)

            if not labels_info:
                self._add_to_failure(label_name, 'No matching labels found')
                return

            best_match = find_best_match(label_name, labels_info)
            if not best_match:
                self._add_to_failure(label_name, 'No best match found')
                return

            label_info = songstats_manager.get_label_info(label_name, best_match)
            if not label_info:
                self._add_to_failure(label_name, 'Could not retrieve label info')
                return

            if not label_info.get('links'):
                self._add_to_failure(label_name, f'Could not find links for {label_name}')
            else:
                self._add_to_label_info(label_row, label_info)
                self._add_label_info_to_success(label_row)

        except Exception as e:
            self._handle_exception(label_name, e)

    def _process_label_content_from_links(self, label: Dict[str, Any]):
        try:
            label_name, label_row = self._get_label_info(label)
            if not label_row:
                return
            self._process_label_for_links(label, label_name, label_row)
            self._add_label_info_to_success(label_row)
        except Exception as e:
            self._handle_exception(label_name, e)

    def _process_label_for_vinyls(self, label: Dict[str, Any]):
        try:
            label_name, label_row = self._get_label_info(label)
            if not label_row:
                return
            bandcamp_manager = BandcampManager()
            labels_info = bandcamp_manager.get_bandcamp_info(label_name)
            if not labels_info:
                self._add_to_failure(label_name, 'No matching labels found')
                return
            best_match = find_best_match(label_name, labels_info, 90)
            if not best_match:
                self._add_to_failure(label_name, 'No best match found')
                return
            else:
                self._add_to_label_info(label_row, best_match)
                self._add_label_info_to_success(label_row)
        except Exception as e:
            self._handle_exception(label_name, e)

    def _get_label_info(self, label: Dict[str, Any]) -> tuple:
        label_name = label.get('name', 'Unknown')
        label_row = label.get('row', 'Unknown')
        self.logger.info(f'Processing label: {label_name} -> in row: {label_row}')
        if not label_row:
            self._add_to_failure(label_name, 'Row not found')
            return label_name, None
        return label_name, label_row

    def _process_label_for_links(self, label: Dict[str, Any], label_name: str, label_row: int):
        processing_type_links = [TypeLink.BEATPORT_URL, TypeLink.SOUNDCLOUD_URL]
        for type_link in processing_type_links:
            url = label.get(type_link.name)
            if not url:
                continue

            self.logger.info(f'Processing {type_link.name} for {label_name}')
            match type_link:
                case TypeLink.BEATPORT_URL:
                    beatport_manager = BeatportManager()
                    label_info = beatport_manager.get_beatport_info(url, label_name)
                case TypeLink.SOUNDCLOUD_URL:
                    soundcloud_manager = SoundcloudManager()
                    label_info = soundcloud_manager.get_soundcloud_info(url, label_name)
                case _:
                    self.logger.warning(f'No manager found for {type_link.name}')
                    continue

            if label_info:
                self._add_to_label_info(label_row, label_info)
            else:
                self._add_to_failure(label_name, f'No {type_link.name} info found')

    def _add_to_label_info(self, label_row: int, label_info: Dict[str, Any]):
        with self.labels_lock:
            if label_row not in self.labels_info:
                self.labels_info[label_row] = {'row': label_row, 'label': {}}
            self.labels_info[label_row]['label'].update(label_info)

    def _add_to_failure(self, label_name: str, reason: str):
        with self.labels_lock:
            self.labels_in_failure.append({'name': label_name, 'reason': reason})

    def _add_label_info_to_success(self, label_row: int):
        with self.labels_lock:
            if label_row in self.labels_info:
                self.labels_in_success.append(self.labels_info[label_row])
                del self.labels_info[label_row]

    def _handle_exception(self, label_name: str, e: Exception):
        self.logger.error(f'Error processing label {label_name}: {str(e)}')
        self._add_to_failure(label_name, str(e))
