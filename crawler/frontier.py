import os
import shelve
from threading import RLock
from queue import Queue, Empty
from urllib.parse import urlparse
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier:
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.lock = RLock()

        self.to_be_downloaded = Queue()
        self.url_map = {}

        # Tracking unique pages and uci.edu subdomains
        self.unique_pages = set()
        self.uci_subdomains = {}

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(f"Did not find save file {self.config.save_file}, starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)

        self.save = shelve.open(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        total_count = len(self.save)
        tbd_count = 0
        with self.lock:
            for urlhash in self.save:
                url, visited = self.save[urlhash]
                self.url_map[urlhash] = (url, visited)
                if not visited and is_valid(url):
                    self.to_be_downloaded.put(url)
                    tbd_count += 1
        self.logger.info(f"Found {tbd_count} URLs to be downloaded from {total_count} total.")

    def get_tbd_url(self):
        try:
            with self.lock:
                return self.to_be_downloaded.get_nowait()
        except Empty:
            self.logger.info("No URL to download. Worker might be idle or done.")
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.url_map:
                self.url_map[urlhash] = (url, False)
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)

    def mark_url_complete(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.url_map:
                self.logger.error(f"Completed URL {url}, but have not seen it before.")
            self.url_map[urlhash] = (url, True)
            self.save[urlhash] = (url, True)
            self.save.sync()
    
            # Track unique pages (without fragment)
            self.unique_pages.add(url)
    
            # Track subdomains
            parsed = urlparse(url)
            if parsed.hostname and parsed.hostname.endswith(".uci.edu"):
                self.uci_subdomains[parsed.hostname] = self.uci_subdomains.get(parsed.hostname, 0) + 1
    
            self.completed_count = getattr(self, 'completed_count', 0) + 1
            if self.completed_count % 50 == 0:
                self.logger.info(f"{self.completed_count} URLs completed so far.")
    
            # Print summary report after each URL
            self.print_summary()

    def mark_url_invalid(self, url):
        urlhash = get_urlhash(url)
        with self.lock:
            self.url_map[urlhash] = (url, True)
            self.save[urlhash] = (url, True)
            self.save.sync()

    def print_summary(self):
        print("\nCrawl Summary Report")
        print("--------------------")
        print(f"Total unique pages found: {len(self.unique_pages)}")

        from scraper import largestPageWordCount, get_top_50_words

        print(f"\nLongest page (by word count): {largestPageWordCount[0]} ({largestPageWordCount[1]} words)")
        print("\nTop 50 most common words (excluding stopwords):")
        for word, count in get_top_50_words():
            print(f"{word}: {count}")

        print("\nUnique subdomains in uci.edu:")
        for subdomain in sorted(self.uci_subdomains):
            print(f"{subdomain}, {self.uci_subdomains[subdomain]}")
