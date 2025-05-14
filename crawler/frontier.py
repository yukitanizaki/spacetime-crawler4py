# frontier.py
import os
import shelve
import time
from threading import RLock
from queue import Queue, Empty
from urllib.parse import urlparse
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid, get_largest_page, all_words
from collections import defaultdict


class Frontier:
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.lock = RLock() # Using self.lock ensures that only one thread at a time can execute the code inside that block
        # no other thread can interrupt or access those shared resources until the current one finishes (like self.url_map or self.save)
        self.domain_count = defaultdict(int) #defaultdict (automatically creates the key and assigns it a default value) 
        #dictionary to keep track of pages per subdomain
        self.start_time = time.time() #start time to crawl
        self.crawled_urls = 0 #total number urls crawled
        self.workers = 0 # This is to print out the final summary, only works with 3 workers

        self.to_be_downloaded = Queue() #queue instead of list
        self.url_map = {} #store urls and whether visited

        self.unique_pages = set() #successfully crawled urls
        self.uci_subdomains = set() #subdomains visited

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, starting from seed.")
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
        total_count = len(self.save)
        tbd_count = 0
        with self.lock:
            for urlhash in self.save: #load urls from save file
                url, visited = self.save[urlhash]
                self.url_map[urlhash] = (url, visited)
                if not visited and is_valid(url): #if not visitid and valid, add to download queue
                    self.to_be_downloaded.put(url)
                    tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} URLs to be downloaded from {total_count} total.")

    # https://stackoverflow.com/questions/16567958/when-and-how-to-use-pythons-rlock
    def get_tbd_url(self):
        try:
            with self.lock:
                return self.to_be_downloaded.get_nowait() #get url from queue without waiting
        except Empty:
            self.workers += 1 #increment number of idle workers
            if self.workers >= 3: # If you change the number of workers from 3, change this too
                self.print_final_summary() #print summary if all three workers done
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.url_map: #if new URL, store as unvisited and add to queue
                self.url_map[urlhash] = (url, False)
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.put(url)

    def mark_url_complete(self, url):
        self.crawled_urls += 1
        if self.crawled_urls % 25 == 0:
            self.print_summary(url) #print summary every 25 urls crawled
        urlhash = get_urlhash(url)
        with self.lock:
            if urlhash not in self.url_map: #log if url not previously seen
                # This should not happen.
                self.logger.error(f"Completed URL {url}, but have not seen it before.")
            self.url_map[urlhash] = (url, True)
            self.save[urlhash] = (url, True)
            self.save.sync()
            self.unique_pages.add(url) #mark url visited and update shelve file

            parsed = urlparse(url)
            if parsed.hostname and parsed.hostname.endswith(".uci.edu"):
                self.uci_subdomains.add(parsed.hostname)
                self.domain_count[parsed.hostname] += 1 #track subdomains

    def mark_url_invalid(self, url): #even if url invalid, mark as visited
        urlhash = get_urlhash(url)
        with self.lock:
            self.url_map[urlhash] = (url, True)
            self.save[urlhash] = (url, True)
            self.save.sync()

    def print_summary(self, url): #print every 25 urls
        print("\nCrawl Summary Report")
        print("--------------------")
        print(f"Total unique pages found: {len(self.unique_pages)}")
        print(f"Total unique subdomains in uci.edu domain: {len(self.uci_subdomains)}")
        print(url)
        # print(sorted(self.uci_subdomains))
        for subdomain in sorted(self.domain_count.keys()): #list all subdomains and count
            print(f"{subdomain}, {self.domain_count[subdomain]}")
        elapsed_time = time.time() - self.start_time
        urls_per_second = self.crawled_urls / elapsed_time if elapsed_time > 0 else 0
        print(f"URLs crawled per second: {urls_per_second:.2f}")
        print(f"Elapsed time: {elapsed_time:.2f} seconds")
        #show speed and time statistics

    def print_final_summary(self): #final summary report
        print("\nFinal Summary Report")
        print("--------------------")
        print(f"Total unique pages found: {len(self.unique_pages)}")
        print(f"Total unique subdomains in uci.edu domain {len(self.uci_subdomains)}")
        # print(sorted(self.uci_subdomains))
        for subdomain in sorted(self.domain_count.keys()):
            print(f"{subdomain}, {self.domain_count[subdomain]}") #list subdomain statistics
        longest_page_url, longest_page_words = get_largest_page()
        print(f"Longest page: {longest_page_url} with {longest_page_words} words") #print largest page

        most_common_words = sorted(all_words.items(), key=lambda item: item[1], reverse=True)[:50]
        print("50 most common words (ignoring stop words):") #print 50 most common words
        for word, count in most_common_words:
            print(f"{word}: {count}")
