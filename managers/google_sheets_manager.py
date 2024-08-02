from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from constants import OUI, NON
from enums import TypeLink
from loggers import AppLogger


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

    def read_column(self, range_name):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=range_name).execute()
            values = result.get('values', [])
            return [item[0] for item in values if item]  # Flatten the list and remove empty entries
        except Exception as e:
            self.logger.error(f'Error reading column {range_name}: {e}')
            return []

    def prepare_batch_updates_for_songstats(self, labels_in_success):
        updates = []
        for success_info in labels_in_success:
            try:
                row = success_info['row']
                label_info = success_info['label']
                column_updates = [
                    {'range': f'Labels!B{row}', 'values': [[label_info.get('country', '')]]},
                    {'range': f'Labels!O{row}', 'values': [[label_info['links'].get(TypeLink.SOUNDCLOUD_URL.value, '')]]},
                    {'range': f'Labels!P{row}', 'values': [[label_info['links'].get(TypeLink.FACEBOOK_URL.value, '')]]},
                    {'range': f'Labels!Q{row}', 'values': [[label_info['links'].get(TypeLink.INSTAGRAM_URL.value, '')]]},
                    {'range': f'Labels!R{row}', 'values': [[label_info['links'].get(TypeLink.BEATPORT_URL.value, '')]]},
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
