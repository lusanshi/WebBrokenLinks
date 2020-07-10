import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from urllib.parse import urlsplit
from typing import Tuple
from requests_html import HTMLSession


@dataclass
class BrokenLinks:
    root: str
    concurrency: int
    depth: int
    max_pages: int
    max_tries: int
    timeout: int
    proxy: Tuple[dict]
    _queue = Queue()
    _processing = True
    _success = set()
    _failed = set()
    _map = {}

    def check(self):
        self._queue.put_nowait((self.root, 0, 0))
        executor = ThreadPoolExecutor(self.concurrency)
        executor.map(self.worker, range(self.concurrency))
        logging.info("Starting %d threads", self.concurrency)
        self._queue.join()
        self._processing = False
        return self.report()

    def worker(self, index):
        session = HTMLSession(verify=False)
        session.proxies = self.proxy
        while self._processing:
            url, redirects, tries = self._queue.get()
            if redirects <= self.depth and len(self._success) + len(self._failed) < self.max_pages:
                r = session.get(url, timeout=self.timeout)
                if r.status_code == 200:
                    self._success.add(url)
                    links = r.html.absolute_links - self._success
                    self._map[url] = links
                    for i in filter(self.url_allowed, links - self._failed):
                        self._queue.put((i, redirects + 1, 0))
                elif tries < self.max_tries:
                    self._queue.put((url, redirects, tries + 1))
                else:
                    self._failed.add(url)
            self._queue.task_done()
        logging.info("Thread %d exit", index)

    def url_allowed(self, url):
        return urlsplit(url).netloc in self.root

    def report(self):
        results = []
        for i in self._failed:
            results.append((i, list(filter(lambda x: i in self._map[x], self._map))))
        return results
