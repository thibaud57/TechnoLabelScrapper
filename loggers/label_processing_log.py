import os
import platform
import tempfile
import time

from constants import MAX_RETRIES
from enums import OperationSystemName


class LabelProcessingLog:
    def __init__(self, tracks_in_success, tracks_in_failure, original_length):
        self.labels_in_success = tracks_in_success
        self.labels_in_failure = tracks_in_failure
        self.original_length = original_length
        self.log_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', suffix='.txt')
        self.log_file_path = self.log_file.name

    def write_log(self):
        try:
            with open(self.log_file_path, 'w', encoding='utf-8') as log_file:
                if len(self.labels_in_failure) > 0:
                    log_file.write(
                        f"Labels failed ({len(self.labels_in_failure)}/{self.original_length}):\n\n")
                    for label in self.labels_in_failure:
                        name = label.get("name", "Unknown")
                        reason = label.get('reason', "Unknown")
                        log_file.write(f"Label: {name} -> {reason}\n")
                if len(self.labels_in_success) > 0:
                    log_file.write(
                        f'\nLabels successfully completed ({len(self.labels_in_success)}/{self.original_length}):\n\n')
                    for label in self.labels_in_success:
                        label_info = label.get("label", "Unknown")
                        name = label_info.get("name", "Unknown")
                        log_file.write(f"Label: {name}\n")
        except IOError as e:
            print(f'Error while writing logs: {e}')

    def open_log_file(self):
        try:
            system_name = platform.system()
            if system_name == OperationSystemName.WINDOWS.value:
                os.startfile(self.log_file_path)
            elif system_name == OperationSystemName.DARWIN.value:
                os.system(f'open "{self.log_file_path}"')
            elif system_name == OperationSystemName.LINUX.value:
                os.system(f'xdg-open "{self.log_file_path}"')
            else:
                print(f'Sorry, your system {system_name} is not supported right now.')

            input("Press Enter after closing the log file to delete it...")
        finally:
            self.cleanup()

    def cleanup(self, delay=5):
        for attempt in range(MAX_RETRIES):
            try:
                if not self.log_file.closed:
                    self.log_file.close()
                os.unlink(self.log_file_path)
                print(f'Temporary log file {self.log_file_path} has been deleted.')
                return
            except OSError as e:
                if attempt < MAX_RETRIES - 1:
                    print(f'Attempt {attempt + 1} failed. Retrying in {delay} seconds...')
                    time.sleep(delay)
                else:
                    print(f'Error while deleting temporary log file after {MAX_RETRIES} attempts: {e}')
                    print(f'Please manually delete the file: {self.log_file_path}')
