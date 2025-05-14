# scraper.py
import re
import hashlib
import math # for cosine similarity calculations
import time
from urllib.parse import urlparse, urljoin, urldefrag
from urllib.robotparser import RobotFileParser
from collections import Counter
from bs4 import BeautifulSoup
from utils.download import download
from utils.server_registration import get_cache_server # register with the cache server
from configparser import ConfigParser

STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
    "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
    "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd",
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that",
    "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this",
    "those", "through", "to", "too", "under", "until", "up", "very", "was",
    "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
    "what's", "when", "when's", "where", "where's", "which", "while", "who",
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
]) #stop words to filter out

# Config and global states
cparser = ConfigParser() # loading the configuration
cparser.read("config.ini")

from utils.config import Config # setting up the configuration
config = Config(cparser)
config.cache_server = get_cache_server(config, False) #registers with cache server
USER_AGENT = config.user_agent #sets user agent string used for crawling

# global variables
visited_hashes = set()  # store content hashes of pages we've seen
visited_texts = []  # list of counters for cosine similarity comparisons
visited_urls = set()  # help track all visited URLs
all_words = {}  # dict to store all word frequencies across crawled pages
ICS_subdomains = {}  # track frequency of each ICS subdomain visited
robot_parsers = {}  # caches parsed robots.txt rules per domain
crawl_delays = {}  # stores crawl delays from robots.txt per domain
_largestPageWordCount = ("", 0)  # tracks largest page by word count (url, count)

# Main scraper
def scraper(url, resp): # main scraper function
    url, _ = urldefrag(url) #removed fragment from url

    if 300 <= resp.status < 400:  # skip if the URL is redirection (3xx)
        return []

    if not is_valid(url) or resp.status != 200: #skip if invalid or http status is not okay (200)
        return []

    if 'Content-Length' in resp.raw_response.headers: #if content larger than 2MB
        content_length = int(resp.raw_response.headers['Content-Length'])
        if content_length > 2 * 1024 * 1024: #2MB (avoid heavy pages)
            return []

    obey_crawl_delay(url) # obey crawl delay rules from robots.txt

    content = resp.raw_response.content # get raw HTML content + compute the checksum value for exact duplicate detection
    checksum = hashlib.sha256(content).hexdigest() # https://docs.python.org/3/library/hashlib.html

    if checksum in visited_hashes: # if exact same content has already been seen, skip; exact duplicate
        print(f"Exact duplicate skipped: {url}")
        return []
    visited_hashes.add(checksum)

    # parse html using beautifulsoup: 
    # https://realpython.com/beautiful-soup-web-scraper-python/#step-3-parse-html-code-with-beautiful-soup
    soup = BeautifulSoup(content, 'html.parser') # parsing the HTML with beautifulsoup
    if not soup.body: # if there is no <body> tag, skip
        return []

    # clean + tokenize the text from the page
    strings = [string.lower() for strings in soup.body.stripped_strings for string in strings.split()] #get all strings in body, lowercase, split into individual words
    if len(strings) < 200: # Skip if page has too few words
        print("Thin page skipped: ", url)
        return []

    counter = Counter(strings) # Count word frequencies for near-duplicate detection
    if is_near_duplicate(counter): # skip if the page is near-duplicate - meaning cosine similarity above threshold
        print(f"Near duplicate skipped: {url}")
        return []
    visited_texts.append(counter)

    update_word_stats(strings) # update global word frequency stats
    track_ics_subdomains(url)  # track ICS subdomains visited
    update_largest_page(url, len(strings)) # track largest page by word count

    links = extract_links(soup, url) # extract all valid links from the page
    if is_root_url(url): # if root page, extract more links from sitemap
        links += extract_next_links(url)

    return [link for link in links if is_valid(link)] # return only the links that are valid according to rules

def extract_next_links(url):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of
    # problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = [] #empty list to store valid links
    parsed = urlparse(url) #parse url into components (scheme, netloc, path, etc)
    if parsed.netloc not in robot_parsers: #if site's robots.txt not parsed yet, don't crawl further
        return []

    sitemaps = robot_parsers[parsed.netloc].site_maps() #get sitemaps urls listed in robots.txt

    if sitemaps:
        print(f"Found sitemaps for {parsed.netloc}: {sitemaps}")
        for sitemap_url in sitemaps:
            try:
                resp = download(sitemap_url, config) #download sitemap
                if not resp or not resp.raw_response or not resp.raw_response.content:
                    print(f"Failed to download sitemap: {sitemap_url}")
                    continue

                soup = BeautifulSoup(resp.raw_response.content, "xml") #try XML first
                locations = soup.find_all("loc") #get all <loc> elements

                if not locations:
                    soup = BeautifulSoup(resp.raw_response.content, "html.parser") #fallback to html
                    locations = soup.find_all("loc")

                for loc in locations:
                    if loc.string:
                        links.append(loc.string.strip()) #extract and store urls
            except Exception as e:
                print(f"Error parsing sitemap {sitemap_url}: {e}")
                continue
    return links

def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        url, _ = urldefrag(url) #remove url fragment
        parsed = urlparse(url) #parse url
        if parsed.scheme not in {"http", "https"}:
            return False
        domain = parsed.netloc.lower() #gets domain
        path = parsed.path.lower() #gets path
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
            return False # we added exe and tar, changed to search to match anywhere in the string
        # using the robot parser to check if we can get this url
        if parsed.netloc in robot_parsers and not robot_parsers[parsed.netloc].can_fetch(USER_AGENT, url): #checks domain has robots.txt and scraper allowed to crawl url
            print(f"Blocked by robots.txt: {url}")
            return False
        return True
    except:
        return False

# Helper functions

def get_largest_page():
    return _largestPageWordCount #url, wordcount

# check if url is the root of a domain
def is_root_url(url):
    parsed = urlparse(url)
    return parsed.path in ["", "/"]

# handle crawl delay using robots.txt
# https://www.w3resource.com/python-exercises/advanced/implement-a-multi-threaded-web%20scraper.php
def obey_crawl_delay(url):
    parsed = urlparse(url) #parse url
    if parsed.netloc in crawl_delays: #if crawl delay recorded
        delay = float(crawl_delays[parsed.netloc]) #retrieves delay from dictionary
        print(f"Obeying crawl delay of {delay}s for {parsed.netloc}")
        time.sleep(delay) #delay
    elif is_root_url(url) and parsed.netloc not in robot_parsers: #check parsed robots.txt and url root of domain
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt" #builds full urlto domain's robots.txt
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            print(f"Fetching robots.txt from {robots_url}")
            parser.read() #download and parse robots.txt
            robot_parsers[parsed.netloc] = parser #stores parser
            delay = parser.crawl_delay(USER_AGENT)
            if delay:
                print(f"Setting crawl delay of {delay}s for {parsed.netloc}")
                crawl_delays[parsed.netloc] = delay
                time.sleep(delay) #delay
            else:
                crawl_delays[parsed.netloc] = 1.0 #if no delay, 1 second
                time.sleep(1.0)
        except Exception as e: #if exception, default 1 second delay
            print(f"Error fetching robots.txt from {robots_url}: {str(e)}")
            crawl_delays[parsed.netloc] = 1.0
            time.sleep(1.0)

def is_near_duplicate(new_counter, threshold=0.9): # checking if two pages are near duplicate, 0.9 because 90% duplicate
    for prev_counter in visited_texts:
        if cosine_sim(prev_counter, new_counter) >= threshold: #cosine similarity near-duplicate
            return True
    return False

# https://datastax.medium.com/how-to-implement-cosine-similarity-in-python-505e8ec1d823
def cosine_sim(c1, c2): # calculating cosine similarity between two counters
    intersection = set(c1) & set(c2) #find common words between both counters
    dot = sum(c1[x] * c2[x] for x in intersection) # Find the dot product
    norm1 = math.sqrt(sum(v ** 2 for v in c1.values())) # Calculate the magnitude of vector 1
    norm2 = math.sqrt(sum(v ** 2 for v in c2.values())) # Calculate the magnitude of vector 2
    if norm1 == 0 or norm2 == 0: # Checks if either vector has 0 magnitude (no content at all)
        return 0.0
    return dot / (norm1 * norm2) # Returns the cosine similarity

def update_word_stats(words): # updating global word frequency dictionary
    global all_words # This accumulates word frequencies
    for word in words:
        # https://www.geeksforgeeks.org/re-match-in-python/
        if word not in STOP_WORDS and re.match("^[a-zA-Z]+$", word): #ignore stop words and non-alphabetic tokens
            all_words[word] = all_words.get(word, 0) + 1 #increments word count

def track_ics_subdomains(url): # tracking how many times each ics subdomain is visited
    parsed = urlparse(url)
    domain = parsed.netloc
    authority = domain.split(".")[1]
    if authority == "ics":
        ICS_subdomains[domain] = ICS_subdomains.get(domain, 0) + 1

def update_largest_page(url, word_count): # record of largest page encountered by word count
    global _largestPageWordCount
    if word_count > _largestPageWordCount[1]: #if word count greater than current max
        _largestPageWordCount = (url, word_count)

def extract_links(soup, base_url): # Returns http/https links, joins relative URLs to the base URL
    links = []
    # https://oxylabs.io/resources/web-scraping-faq/beautifulsoup/get-href
    for a_tag in soup.find_all("a", href=True): #all anchor <a> tags that have href attribute
        raw_href = a_tag['href'] #get fref value; convert relative links into full URLs, filter and follow valid links
        try:
            joined_url = urljoin(base_url, raw_href) #combines base url and relatve url into full
            joined_url, _ = urldefrag(joined_url) #removes fragments
            if joined_url.startswith('http'): #only http or https
                links.append(joined_url)
        except ValueError as e:
            print(f"Skipping bad href '{raw_href}' at {base_url}: {e}")
            continue
    return links
