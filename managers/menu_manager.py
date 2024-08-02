import time

from constants import MENU_CHOICE_1, MENU_CHOICE_2, EXIT_KEY
from enums.menu_action import MenuAction
from loggers import AppLogger, LabelProcessingLog
from processors import LabelProcessor, TopProcessor


class MenuManager:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.current_choice = None
        self.main_menu_text = '###MENU###\n1: Process labels\n2: Process top 100'
        self.label_menu_text = '###LABEL MENU###\n1: Songstats\n2: Links'

    def display_main_menu(self):
        main_menu_choices = {MENU_CHOICE_1: MenuAction.PROCESS_LABELS.value,
                             MENU_CHOICE_2: MenuAction.PROCESS_TOP100.value}
        choice, action = self._get_user_choice(self.main_menu_text, main_menu_choices)
        if choice is None:
            return
        elif action == MenuAction.PROCESS_LABELS.value:
            self._display_label_menu()
        elif action == MenuAction.PROCESS_TOP100.value:
            self._process_top100()

    def _get_user_choice(self, menu_text, valid_choices):
        while True:
            time.sleep(0.1)
            print(menu_text)
            choice = input(f'Enter your choice (or type {EXIT_KEY} to quit): ')
            if choice.lower() == EXIT_KEY:
                return None, None
            if choice in valid_choices:
                return choice, valid_choices[choice]
            self.logger.info(f'Invalid choice. Please enter a valid option or {EXIT_KEY} to quit.')

    def _display_label_menu(self):
        label_menu_choices = {MENU_CHOICE_1: MenuAction.PROCESS_SONGSTATS.value,
                              MENU_CHOICE_2: MenuAction.PROCESS_LINKS.value}
        choice, action = self._get_user_choice(self.label_menu_text, label_menu_choices)
        if choice is not None:
            self._process_labels(action)

    def _process_labels(self, action):
        labels_processor = LabelProcessor()
        try:
            self.logger.info('###START LABELS PROCESSING###')
            labels_processor.run(action)
        except Exception as e:
            self.logger.error(f'An error occurred while processing the labels: {e}')
        finally:
            if len(labels_processor.labels_in_success) > 0 or len(labels_processor.labels_in_failure) > 0:
                self.logger.info(
                    f'Processing completed. Successes: {len(labels_processor.labels_in_success)}, Failures: {len(labels_processor.labels_in_failure)}')
                self._handle_logs(labels_processor)
            else:
                self.logger.warning('No labels were processed successfully or failed.')
        self.logger.info('###END LABELS PROCESSING###')

    def _process_top100(self):
        top_processor = TopProcessor()
        # try:
        #     self.logger.info('###START TRACKS PROCESSING###')
        #     top_processor.run()
        # except Exception as e:
        #     self.logger.error(f'An error occurred while processing the tracks: {e}')
        # finally:
        #     if len(track_processor.tracks_in_success) > 0 or len(track_processor.tracks_in_failure) > 0:
        #         self._handle_logs(track_processor)
        #     self.logger.info('###END TRACKS PROCESSING###')

    def _handle_logs(self, processor):
        if len(processor.labels_in_success) > 0 or len(processor.labels_in_failure) > 0:
            self.logger.info('Writing logs')
            processing_log = LabelProcessingLog(processor.labels_in_success, processor.labels_in_failure,
                                                processor.total_labels_to_proceed)
            processing_log.write_log()
            processing_log.open_log_file()
        else:
            self.logger.warning('No labels to process')
