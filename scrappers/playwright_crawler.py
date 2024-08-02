import random
from typing import Tuple, Optional, List, Any

from playwright.sync_api import Playwright as PlaywrightInstance
from playwright.sync_api import sync_playwright, Page, BrowserContext, Browser, Request

from constants import USER_AGENTS
from loggers import AppLogger


class RequestInterceptor:
    def __init__(self, url_pattern: str):
        self.url_pattern = url_pattern
        self.requests: List[Request] = []

    def __call__(self, request: Request):
        if self.url_pattern in request.url:
            self.requests.append(request)


class PlaywrightCrawler:
    def __init__(self):
        self.logger = AppLogger().get_logger()
        self.user_agents = USER_AGENTS

    def scrape(self, url: str, wait_selector: Optional[str] = None) -> str | dict[str, Any] | None:
        with sync_playwright() as p:
            browser, context, page = self.init_playwright_config(p)
            try:
                page.goto(url)
                if wait_selector:
                    page.wait_for_selector(wait_selector, state='visible')
                content = page.content()
                return content
            except Exception as e:
                self.logger.error(f'An error occurred: {e}')
                return None
            finally:
                self.close_connection(browser, context)

    def init_playwright_config(self, p: PlaywrightInstance) -> Tuple[Browser, BrowserContext, Page]:
        user_agent = random.choice(self.user_agents)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='fr-FR',
            timezone_id='Europe/Paris'
        )
        page = context.new_page()
        page.set_extra_http_headers({
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.google.com/'
        })
        return browser, context, page

    def close_connection(self, browser: Browser, context: BrowserContext):
        context.close()
        browser.close()
