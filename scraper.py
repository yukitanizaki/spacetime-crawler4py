import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    links = [] #empty list to store valid links
    try:
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser') #parse html with Beautiful Soup
        raw_links = [a.get('href') for a in soup.find_all('a', href=True #find <a> tags with href attribute and extract values
        for raw_link in raw_links:
            absolute_link = urljoin(url, raw_link) #convert relative links to absolute urls
            parsed_link = urlparse(absolute_link) #break down url into components
            clean_link = parsed_link._replace(fragment='', query='').geturl() #remove fragments and queries
            links.append(clean_link) #add cleaned url to list
    except Exception as e: #parsing error
        print(f"Error while parsing {url}: {e}")
    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        domain = parsed.netloc.lower() #lowercase
        path = parsed.path.lower() #lowercase

        if not (
            domain.endswith("ics.uci.edu") or
            domain.endswith("cs.uci.edu") or
            domain.endswith("informatics.uci.edu") or
            domain.endswith("stat.uci.edu") or
            (domain == "today.uci.edu" and path.startswith("/department/information_computer_sciences"))
        ): #specific ics domains
            return False

        if re.search(r"(calendar|event|events|/\d{4}/\d{2}/\d{2})", path): #events, calendars, dates
            return False

        if path.count('/') > 10: #deep urls
            return False

        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
        return False
        
    except TypeError:
        print("TypeError for ", parsed)
        return False
