# scraper.py
import re 
import hashlib
from bs4 import BeautifulSoup

import math # for cosine similarity calculations
import time 
from urllib.parse import urlparse, urljoin, urldefrag 
from urllib.robotparser import RobotFileParser 
from collections import Counter 
from utils.download import download 
from utils.server_registration import get_cache_server # register with the cache server
from configparser import ConfigParser 

cparser = ConfigParser()  # loading the configuration
cparser.read("config.ini")

from utils.config import Config # setting up the configuration
config = Config(cparser)
config.cache_server = get_cache_server(config, False)
USER_AGENT = config.user_agent # setting up the user agent

# global variables
visited_hashes = set()  # store content hashes of pages we've seen
visited_texts = []  # list of counters for cosine similarity comparisons
visited_urls = set()  # help track all visited URLs
all_words = {}  # dict to store all word frequencies across crawled pages
ICS_subdomains = {}  # track frequency of each ICS subdomain visited
robot_parsers = {}  # caches parsed robots.txt rules per domain
crawl_delays = {}  # stores crawl delays from robots.txt per domain
largestPageWordCount = ("", 0)  # tracks largest page by word count (url, count)

def scraper(url, resp): # main scraper function
    if not is_valid(url) or resp.status != 200:  # skip if the URL is invalid or response is not ok (200 status)
        return []

    obey_crawl_delay(url) # obey crawl delay rules from robots.txt


    content = resp.raw_response.content # get raw HTML content + compute the checksum value for exact duplicate detection
    checksum = hashlib.sha256(content).hexdigest()


    if checksum in visited_hashes: # if exact same content has already been seen, skip
        print(f"Exact duplicate skipped: {url}")
        return []
    visited_hashes.add(checksum)

    # parse html using beautifulsoup
    soup = BeautifulSoup(content, 'html.parser') # parsing the HTML with beautifulsoup
    if not soup.body: # if there is no body, skip
        return []

    # clean + tokenize the text from the page
    strings = [string.lower() for strings in soup.body.stripped_strings for string in strings.split()]
    if len(strings) < 150: # Skip if page has too few words
        print("Thin page skipped:", url)
        return []

    counter = Counter(strings) # Count word frequencies for near-duplicate detection

    # skip if the page is near-duplicate - meaning cosine similarity above threshold
    if is_near_duplicate(counter): 
        print(f"Near duplicate skipped: {url}")
        return []
    visited_texts.append(counter)

    update_word_stats(strings) # update global word frequency stats
    track_ics_subdomains(url)  # track ICS subdomains visited
    update_largest_page(url, len(strings)) # track largest page by word count

    links = extract_next_links(soup, url) # extract all valid links from the page
  
    if is_root_url(url): # if root page, extract more links from sitemap
        links += sitemap_links_if_available(url)

    return [link for link in links if is_valid(link)] # return only the links that are valid according to rules

# handle crawl delay using robots.txt
def obey_crawl_delay(url):
    parsed = urlparse(url)

    if parsed.netloc in crawl_delays: # if crawl delay is known, wait accordingly
        time.sleep(max(float(crawl_delays[parsed.netloc]) - 0.5, 0))

    elif is_root_url(url) and parsed.netloc not in robot_parsers: # otherwise, parse robots.txt to get crawl delay

        parser = RobotFileParser() 
        parser.set_url(urljoin(url, 'robots.txt'))
        try:
            parser.read()
            robot_parsers[parsed.netloc] = parser
            delay = parser.crawl_delay(USER_AGENT)
            if delay:
                crawl_delays[parsed.netloc] = delay
        except:
            pass

def is_valid(url): 
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        domain = parsed.netloc.lower()
        path = parsed.path.lower()
        if not ( # restricting to specific subdomains and dept
            domain.endswith(".ics.uci.edu") or
            domain.endswith(".cs.uci.edu") or
            domain.endswith(".informatics.uci.edu") or
            domain.endswith(".stat.uci.edu") or
            ("today.uci.edu" in domain and "/department/information_computer_sciences" in path)
        ):
            return False
        # no common binary/media file extensions
        if re.search(r"\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mp2|mp3|mp4|avi|mov|pdf|docx?|xlsx?|pptx?|zip|rar|gz|exe|tar)$", path):
            return False
        # using the robot parser to check if we can get this url
        if parsed.netloc in robot_parsers and not robot_parsers[parsed.netloc].can_fetch(USER_AGENT, url):
            return False
        return True
    except:
        return False

def is_near_duplicate(new_counter, threshold=0.9): # checking if two pages are near duplicare
    for prev_counter in visited_texts:
        if cosine_sim(prev_counter, new_counter) >= threshold:
            return True
    return False

def cosine_sim(c1, c2): # calculating cosine similarity between two counters
    intersection = set(c1) & set(c2)
    dot = sum(c1[x] * c2[x] for x in intersection)
    norm1 = math.sqrt(sum(v ** 2 for v in c1.values()))
    norm2 = math.sqrt(sum(v ** 2 for v in c2.values()))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)

def update_word_stats(words): # updating global word frequency dictionary
    for word in words:
        all_words[word] = all_words.get(word, 0) + 1

def track_ics_subdomains(url): # tracking how many times each ics subdomain is visited
    parsed = urlparse(url)
    domain = parsed.netloc
    authority = domain.split(".")[1]
    if authority == "ics":
        ICS_subdomains[domain] = ICS_subdomains.get(domain, 0) + 1

def update_largest_page(url, count): # record of largest page encountered by word count
    global largestPageWordCount
    if count > largestPageWordCount[1]:
        largestPageWordCount = (url, count)

def extract_next_links(soup, base_url): #extracting all links from the page that use the http/https scheme
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = []
    for a_tag in soup.find_all("a", href=True):
        raw_href = a_tag['href']
        try:
            joined_url = urljoin(base_url, raw_href)
            if joined_url.startswith('http'):
                links.append(joined_url)
        except ValueError as e:
            print(f"Skipping bad href '{raw_href}' at {base_url}: {e}")
            continue
    return links

# if sitemap urls are available in robots.txt, try to extract links from them
def sitemap_links_if_available(url):
    parsed = urlparse(url)
    if parsed.netloc not in robot_parsers:
        return []
    sitemaps = robot_parsers[parsed.netloc].site_maps()
    links = []
    if sitemaps:
        for sitemap_url in sitemaps:
            resp = download(sitemap_url, config)
            if not resp or not resp.raw_response or not resp.raw_response.content:
                continue
            soup = BeautifulSoup(resp.raw_response.content, "xml")
            for loc in soup.find_all("loc"):
                if loc.string:
                    links.append(loc.string)
    return links

# check if url is the root of a domain
def is_root_url(url):
    parsed = urlparse(url)
    return parsed.path == "" or parsed.path == "/"
