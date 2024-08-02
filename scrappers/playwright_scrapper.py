import random
from typing import List

from playwright.sync_api import Page, Request
from playwright.sync_api import Playwright as PlaywrightInstance

from constants import USER_AGENTS
from loggers import AppLogger


class RequestInterceptor:
    def __init__(self, url_pattern: str):
        self.url_pattern = url_pattern
        self.requests: List[Request] = []

    def __call__(self, request: Request):
        if self.url_pattern in request.url:
            self.requests.append(request)


class PlaywrightScrapper:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.user_agents = USER_AGENTS
        self.context = None
        self.browser = None

    def init_playwright_page(self, p: PlaywrightInstance) -> Page:
        user_agent = random.choice(self.user_agents)
        self.browser = p.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='fr-FR',
            timezone_id='Europe/Paris'
        )
        page = self.context.new_page()
        page.set_extra_http_headers({
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.google.com/'
        })
        return page

    def close_connection(self):
        self.context.close()
        self.context = None
        self.browser.close()
        self.browser = None
