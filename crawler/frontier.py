# frontier.py
import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty
from urllib.parse import urlpars
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier:
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.lock = RLock() #to lock urls
        self.to_be_downloaded = Queue() #queue instead of list
        self.url_map = {} #dict to keep urls

        #tracking unique pages and uci.edu subdomains
        self.unique_pages = set()
        self.uci_subdomains = set()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        with self.lock: #lock so only one thread access
            for urlhash in self.save:
                url, visited = self.save[urlhash] #access urls in save dict
                self.url_map[urlhash] = (url, visited) #update url and visited status
                if not visited and is_valid(url):
                    self.to_be_downloaded.put(url) #add to queue
                    tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            with self.lock: #lock so only one thread access
                return self.to_be_downloaded.get_nowait() #retreive url from queue without blocking
        except Empty:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock: #lock so only one thread access
            if urlhash not in self.url_map:
                self.url_map[urlhash] = (url, False) #update url and visited status
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url) #add to queue

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self.lock: #lock so only one thread access
            if urlhash not in self.url_map:
                # This should not happen.
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
                
            self.url_map[urlhash] = (url, True) #update url and visited status
            self.save[urlhash] = (url, True)
            self.save.sync()

            self.unique_pages.add(url) #track unique pages

            parsed = urlparse(url) #track uci.edu subdomains
            if parsed.hostname and parsed.hostname.endswith(".uci.edu"):
                self.uci_subdomains.add(parsed.hostname)
    
    def mark_url_invalid(self, url):
        urlhash = get_urlhash(url) #get hash
        with self.lock: #lock so only one thread access
            self.url_map[urlhash] = (url, True) #update url and visited status
            self.save[urlhash] = (url, True)
            self.save.sync()

    def print_summary(self):
        print("\nCrawl Summary Report")
        print("--------------------")
        print(f"Total unique pages found: {len(self.unique_pages)}")
        print(f"Total unique subdomains in uci.edu domain: {len(self.uci_subdomains)}")
        for subdomain in sorted(self.uci_subdomains):
            print(subdomain)
