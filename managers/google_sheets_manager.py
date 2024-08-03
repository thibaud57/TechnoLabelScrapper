from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from constants import OUI, NON
from enums import TypeLink
from loggers import AppLogger
from utils.utils import extract_number


class GoogleSheetsManager:
    def __init__(self, credentials_file, spreadsheet_id):
        self.logger = AppLogger.get_logger()
        self.spreadsheet_id = spreadsheet_id
        self.service = self._authenticate(credentials_file)

    def _authenticate(self, credentials_file):
        try:
            credentials = Credentials.from_service_account_file(credentials_file,
                                                                scopes=['https://www.googleapis.com/auth/spreadsheets'])
            return build('sheets', 'v4', credentials=credentials)
        except Exception as e:
            self.logger.error(f'Authentication failed: {e}')
            raise

    def read_columns(self, range_name):
        try:
            ranges = range_name.split(',')
            batch_result = self.service.spreadsheets().values().batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges=ranges
            ).execute()
            value_ranges = batch_result.get('valueRanges', [])
            merged_data = []
            max_rows = max(len(vr.get('values', [])) for vr in value_ranges)

            start_row = int(ranges[0].split('!')[1].split(':')[0][1:]) if ranges else 1

            for row_index in range(max_rows):
                merged_row = [start_row + row_index]
                for vr in value_ranges:
                    values = vr.get('values', [])
                    if row_index < len(values):
                        merged_row.append(values[row_index][0] if values[row_index] else '')
                    else:
                        merged_row.append('')
                merged_data.append(merged_row)
            return merged_data
        except Exception as e:
            self.logger.error(f'Error reading columns {range_name}: {e}')
            return []

    def prepare_batch_updates_for_songstats(self, labels_in_success):
        updates = []
        for success_info in labels_in_success:
            try:
                row = success_info['row']
                label_info = success_info['label']
                column_updates = [
                    {'range': f'Labels!B{row}', 'values': [[label_info.get('country', '')]]},
                    {'range': f'Labels!O{row}',
                     'values': [[label_info['links'].get(TypeLink.SOUNDCLOUD_URL.name, '')]]},
                    {'range': f'Labels!P{row}', 'values': [[label_info['links'].get(TypeLink.FACEBOOK_URL.name, '')]]},
                    {'range': f'Labels!Q{row}', 'values': [[label_info['links'].get(TypeLink.INSTAGRAM_URL.name, '')]]},
                    {'range': f'Labels!R{row}', 'values': [[label_info['links'].get(TypeLink.BEATPORT_URL.name, '')]]},
                    {'range': f'Labels!U{row}', 'values': [[OUI]]}
                ]
                updates.extend(column_updates)
                self.logger.debug(f"Prepared updates for label: {label_info.get('name', 'Unknown')} at row {row}")
            except KeyError as e:
                self.logger.error(f'KeyError while preparing update: {str(e)}. Label info: {success_info}')
            except Exception as e:
                self.logger.error(f'Unexpected error while preparing update: {str(e)}. Label info: {success_info}')
        self.logger.info(f'Prepared {len(updates)} individual column updates for {len(labels_in_success)} labels')
        return updates

    def prepare_batch_updates_for_links(self, labels_in_success):
        updates = []
        for success_info in labels_in_success:
            try:
                row = success_info['row']
                label_info = success_info['label']
                column_updates = [
                    {'range': f'Labels!D{row}', 'values': [[label_info.get('actif', NON)]]},
                    {'range': f'Labels!E{row}', 'values': [[label_info.get('ouvert_nouveaux', NON)]]},
                    {'range': f'Labels!F{row}', 'values': [[label_info.get('email_demo', '')]]},
                    {'range': f'Labels!N{row}', 'values': [[label_info.get('soundcloud_followers', '')]]},
                ]
                updates.extend(column_updates)
                self.logger.debug(f"Prepared updates for label: {label_info.get('name', 'Unknown')} at row {row}")
            except KeyError as e:
                self.logger.error(f'KeyError while preparing update: {str(e)}. Label info: {success_info}')
            except Exception as e:
                self.logger.error(f'Unexpected error while preparing update: {str(e)}. Label info: {success_info}')
        self.logger.info(f'Prepared {len(updates)} individual column updates for {len(labels_in_success)} labels')
        return updates

    def prepare_batch_updates_for_beatstats(self, labels):
        updates = []
        for label in labels:
            try:
                row = label['row']
                position = label.get('position', '')
                is_hype = label.get('is_hype', False)
                top_value = f"{position} HYPE" if is_hype else position
                is_new = label.get('is_new', False)
                if is_new:
                    column_updates = [
                        {'range': f'Labels!A{row}', 'values': [[label.get('name', '')]]},
                        {'range': f'Labels!C{row}', 'values': [[label.get('genre', '')]]},
                        {'range': f'Labels!R{row}', 'values': [[label.get(TypeLink.BEATPORT_URL.name, '')]]},
                        {'range': f'Labels!T{row}', 'values': [[top_value]]},
                        {'range': f'Labels!V{row}', 'values': [[OUI]]}
                    ]
                    updates.extend(column_updates)
                else:
                    label_from_sheet =  self.read_columns(f'Labels!T{row},C{row}')[0]
                    top_number = extract_number(top_value)
                    actual_number = extract_number(label_from_sheet[1])
                    best_top = min(top_number, actual_number) if actual_number > 0 else top_number
                    genre = label.get('genre', '') if top_number < actual_number else label_from_sheet[2]
                    column_updates = [
                        {'range': f'Labels!C{row}', 'values': [[genre]]},
                        {'range': f'Labels!R{row}', 'values': [[label.get(TypeLink.BEATPORT_URL.name, '')]]},
                        {'range': f'Labels!T{row}', 'values': [[best_top]]},
                        {'range': f'Labels!V{row}', 'values': [[OUI]]}
                    ]
                    updates.extend(column_updates)
                self.logger.debug(f"Prepared updates for label: {label.get('name', 'Unknown')} at row {row}")
            except KeyError as e:
                self.logger.error(f'KeyError while preparing update: {str(e)}. Label info: {label}')
            except Exception as e:
                self.logger.error(f'Unexpected error while preparing update: {str(e)}. Label info: {label}')
        self.logger.info(f'Prepared {len(updates)} individual column updates for {len(labels)} labels')
        return updates

    def batch_update(self, updates):
        try:
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': updates
            }
            result = self.service.spreadsheets().values().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body).execute()
            self.logger.info(f"Batch update completed. {result.get('totalUpdatedCells')} cells updated.")
            return True
        except HttpError as e:
            self.logger.error(f'HTTP error occurred during batch update: {e}')
            return False
        except Exception as e:
            self.logger.error(f'Unexpected error during batch update: {e}')
            return False
