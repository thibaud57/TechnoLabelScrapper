from enum import Enum


class MenuAction(Enum):
    PROCESS_LABELS = 'process_labels'
    PROCESS_TOP100 = 'process_top100'
    PROCESS_SONGSTATS = 'process_songstats'
    PROCESS_LINKS = 'process_links'
    PROCESS_VINYLS = 'process_vinyls'
