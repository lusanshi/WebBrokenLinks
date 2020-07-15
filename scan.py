import logging
import re
from csv import writer
from dataclasses import dataclass
from json import dumps
from os import sep
from queue import Queue
from threading import Thread
from typing import Tuple
from urllib.parse import urlsplit

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
    _processing: bool = True

    def __post_init__(self):
        self._queue = Queue()
        self._success = set()
        self._failed = set()
        self._seen = set()
        self._map = {}
        self._queue.put_nowait((self.root, 0, 0))

    def check(self):
        threads = []
        for w in range(self.concurrency):
            t = Thread(target=self.worker)
            t.start()
            threads.append(t)
        logging.info("Starting %d threads", self.concurrency)
        self._queue.join()
        self._processing = False
        for t in threads:
            t.join()

    def worker(self):
        session = HTMLSession(verify=False)
        while self._processing:
            url, redirects, tries = self._queue.get()
            if redirects <= self.depth and len(self._seen) < self.max_pages:
                try:
                    r = session.get(url, timeout=self.timeout)
                    if r.status_code == 200:
                        if url in self._success:
                            print(url)
                        self._success.add(url)
                        links = r.html.absolute_links - self._success
                        self._map[url] = links
                        for i in filter(lambda x: urlsplit(self.root).netloc in x, links - self._seen - self._failed):
                            self._seen.add(i)
                            self._queue.put((i, redirects + 1, 0))
                    elif tries < self.max_tries:
                        self._queue.put((url, redirects, tries + 1))
                    else:
                        self._failed.add(url)
                except Exception as err:
                    logging.error(err, exc_info=True)
                    self._failed.add(url)
            self._queue.task_done()

    def result(self):
        results = {}
        for i in self._failed:
            results[i] = list(filter(lambda x: i in self._map[x], self._map))
        return results


def genReport(name, data: dict, dirname):
    filename = f"{validateTitle(name)}.csv"
    with open(f'{dirname}{sep}{filename}', 'w', encoding='utf8') as f:
        o = writer(f)
        o.writerow(['坏链', '定位页面'])
        o.writerows([(k, ', '.join(v)) for k, v in data.items()])
    return filename


def validateTitle(title):
    rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/ \ : * ? " < > |'
    new_title = re.sub(rstr, "_", title)
    return new_title
