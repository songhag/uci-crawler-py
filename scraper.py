import re
from asyncio.sslproto import SSLProtocolState
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter, defaultdict

ALLOWED_HOST_SUFFIXES = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)
ALLOWED_SCHEMES = {"http", "https"}


# Report VAR
SEEN_URLS = set()
PAGE_WORDCOUNT = {}
# Longest page info
LONGEST_PAGE_URL = None
LONGEST_PAGE_WORDS = 0
GLOBAL_WORD_FREQ = Counter()
SUBDOMAIN_PAGECOUNT = defaultdict(int)

# Tokenizer
WORD_RE = re.compile(r"[a-z0-9]+")
# Lightweight trap avoidance
PATH_FAMILY_COUNT = Counter()
CONTENT_HASHES = set()




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

    links = []
    try:
        if resp is None or getattr(resp, "status", None) != 200:
            return links

        raw = getattr(resp, "raw_response", None)
        if raw is None:
            return links

        content = getattr(raw, "content", None)
        if not content:
            return links

        base_url = getattr(raw, "url", None) or url

        soup = BeautifulSoup(content, "lxml")

        for a in soup.find_all("a", href = True):
            href = a.get("href")
            if not href:
                continue

            abs_url = urljoin(base_url, href)
            abs_url = urldefrag(abs_url)[0]

            std_url = _standard_url(abs_url)
            if std_url:
                links.append(std_url)

    except Exception:
        return []

    return list(dict.fromkeys(links))


def _standard_url(u: str):
    """make sure the url satisfy a standard"""
    try:
        u = urldefrag(u)[0]
        parsed = urlparse(u)

        if parsed.scheme not in ALLOWED_SCHEMES:
            return None

        host = (parsed.hostname or "").lower()
        if not host:
            return None

        # remove default ports
        netloc = host
        if parsed.port:
            if (parsed.scheme == "http" and parsed.port != 80) or (parsed.scheme == "https" and parsed.port != 443):
                netloc = f"{host}:{parsed.port}"

        path = parsed.path or "/"
        # normalize multiple slashes
        path = re.sub(r"/{2,}", "/", path)

        if path != "/" and path.endswith("/"):
            path = path[:-1]

        query = parsed.query

        rebuilt = f"{parsed.scheme}://{netloc}{path}"
        if query:
            rebuilt += f"?{query}"
        return rebuilt

    except Exception:
        return None



def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise
