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

from Config.db_conn import DBConn, DB_main


@dataclass
class BrokenLinks:
    root: str
    concurrency: int
    depth: int
    max_pages: int
    max_tries: int
    timeout: int
    # proxy: Tuple[dict]
    _queue = Queue()
    _processing = True
    _success = set()
    _failed = set()
    _seen = set()
    _map = {}

    def check(self):
        self._queue.put_nowait((self.root, 0, 0))
        for w in range(self.concurrency):
            t = Thread(target=self.worker)
            t.start()
        logging.info("Starting %d threads", self.concurrency)
        self._queue.join()
        self._processing = False

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

    def save(self, project_id):
        with DBConn(DB_main) as c:
            c.execute("UPDATE base_info SET urls_len=%s, broken_links=%s WHERE proj_id=%s",
                      (len(self._success) + len(self._failed), dumps(self.result()), project_id))


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


if __name__ == '__main__':
    s = BrokenLinks('http://www.baidu.com/', 5, 3, 2000, 2, 5)
    s.check()
    print(s.result())
