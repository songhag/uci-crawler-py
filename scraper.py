import re
import json
import time
import hashlib

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

STATS_PATH = "crawl_stats.json"

STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are",
    "as", "at", "also",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
    "can", "could",
    "did", "do", "does", "doing", "down", "during",
    "each",
    "few", "for", "from", "further",
    "had", "has", "have", "having", "he", "her", "here", "hers", "herself", "him", "himself", "his",
    "how",
    "i", "if", "in", "into", "is", "it", "its", "itself",
    "just",
    "me", "more", "most", "my", "myself", "may",
    "no", "nor", "not", "now",
    "of", "off", "on", "once", "only", "or", "other", "our", "ours", "ourselves", "out", "over",
    "own",
    "same", "she", "should", "so", "some", "such",
    "than", "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these",
    "they",
    "this", "those", "through", "to", "too",
    "under", "until", "up",
    "very",
    "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "with",
    "would", "will",
    "you", "your", "yours", "yourself", "yourselves",
}


def _load_stats():
    """Load persisted stats if present"""
    global SEEN_URLS, PAGE_WORDCOUNT, LONGEST_PAGE_URL, LONGEST_PAGE_WORDS
    global GLOBAL_WORD_FREQ, SUBDOMAIN_PAGECOUNT, PATH_FAMILY_COUNT, CONTENT_HASHES

    try:
        with open(STATS_PATH, "r", encoding = "utf-8") as f:
            data = json.load(f)

        SEEN_URLS = set(data.get("seen_urls", []))
        PAGE_WORDCOUNT = {k: int(v) for k, v in data.get("page_wordcount", {}).items()}
        LONGEST_PAGE_URL = data.get("longest_page_url")
        LONGEST_PAGE_WORDS = int(data.get("longest_page_words", 0))

        GLOBAL_WORD_FREQ = Counter(data.get("global_word_freq", {}))
        SUBDOMAIN_PAGECOUNT = defaultdict(int, {k: int(v) for k, v in
                                                data.get("subdomain_pagecount", {}).items()})
        PATH_FAMILY_COUNT = Counter(data.get("path_family_count", {}))
        CONTENT_HASHES = set(data.get("content_hashes", []))

    except FileNotFoundError:
        return
    except Exception:
        return


def _save_stats(throttle_seconds = 2.0):
    """Persist stats"""
    now = time.time()
    last = getattr(_save_stats, "_last_save_ts", 0.0)
    if now - last < throttle_seconds:
        return
    _save_stats._last_save_ts = now

    data = {
        "seen_urls": sorted(SEEN_URLS),
        "page_wordcount": PAGE_WORDCOUNT,
        "longest_page_url": LONGEST_PAGE_URL,
        "longest_page_words": LONGEST_PAGE_WORDS,
        "global_word_freq": dict(GLOBAL_WORD_FREQ),
        "subdomain_pagecount": dict(SUBDOMAIN_PAGECOUNT),
        "path_family_count": dict(PATH_FAMILY_COUNT),
        "content_hashes": sorted(CONTENT_HASHES),
    }
    try:
        with open(STATS_PATH, "w", encoding = "utf-8") as f:
            json.dump(data, f, ensure_ascii = False, indent = 2)
    except Exception:
        pass


def scraper(url, resp):
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link)]

    try:
        if resp is None or getattr(resp, "status", None) != 200:
            return valid_links

        raw = getattr(resp, "raw_response", None)
        if raw is None:
            return valid_links

        content = getattr(raw, "content", None)
        if not content:
            return valid_links

        headers = getattr(raw, "headers", {}) or {}
        ctype = ""
        if isinstance(headers, dict):
            ctype = headers.get("Content-Type", "") or headers.get("content-type", "") or ""
        if ctype and ("text/html" not in ctype and "application/xhtml+xml" not in ctype):
            return valid_links

        standard_url = _standard_url(resp.url if getattr(resp, "url", None) else url)
        if standard_url is None:
            return valid_links

        if standard_url in SEEN_URLS:
            return valid_links
        SEEN_URLS.add(standard_url)

        soup = BeautifulSoup(content, "lxml")
        _remove_junk_tags(soup)
        text = soup.get_text(separator = " ", strip = True)
        tokens = _tokenize(text)

        if len(tokens) < 100:
            _save_stats()
            return valid_links

        wc = len(tokens)

        if wc > 200000:
            return valid_links

        alpha_chars = sum(c.isalpha() for c in text)
        alpha_ratio = alpha_chars / max(len(text), 1)
        if alpha_ratio < 0.6:
            return valid_links

        text_hash = hashlib.sha1(text.encode("utf-8", errors = "ignore")).hexdigest()
        if text_hash in CONTENT_HASHES:
            _save_stats()
            return valid_links
        CONTENT_HASHES.add(text_hash)

        wc = len(tokens)
        PAGE_WORDCOUNT[standard_url] = wc
        global LONGEST_PAGE_URL, LONGEST_PAGE_WORDS
        if wc > LONGEST_PAGE_WORDS:
            LONGEST_PAGE_WORDS = wc
            LONGEST_PAGE_URL = standard_url

        for w in tokens:
            if w in STOP_WORDS:
                continue
            if not any(ch.isalpha() for ch in w):
                continue
            GLOBAL_WORD_FREQ[w] += 1

        parsed = urlparse(standard_url)
        host = (parsed.hostname or "").lower()
        if host.endswith("uci.edu"):
            SUBDOMAIN_PAGECOUNT[host] += 1

        _save_stats()

    except Exception:
        return valid_links

    return valid_links


_load_stats()


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

        base_url = getattr(resp, "url", None) or getattr(raw, "url", None) or url

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
            if (parsed.scheme == "http" and parsed.port != 80) or (
                    parsed.scheme == "https" and parsed.port != 443):
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

        # 1. Scheme Check: Only allow http and https [cite: 78]
        if parsed.scheme not in ALLOWED_SCHEMES:
            return False

        # 2. Domain Check: Must be within the specified UCI domains [cite: 17, 53, 121]
        # host is retrieved via parsed.hostname
        host = (parsed.hostname or "").lower()
        if not any(
                host == domain or host.endswith("." + domain) for domain in ALLOWED_HOST_SUFFIXES):
            return False

        # 3. Extension Filtering: Avoid non-text or large files
        # This regex filters out images, archives, and multimedia to save bandwidth.
        if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico|webp"
                + r"|png|tiff?|mid|mp2|mp3|mp4|webm"
                + r"|wav|avi|mov|mpeg|mpg|ram|m4v|mkv|ogg|ogv|pdf"
                + r"|ps|eps|tex|ppt|pptx|pps|ppsx|doc|docx|xls|xlsx|names"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                + r"|epub|dll|cnf|tgz|sha1"
                + r"|thmx|mso|arff|rtf|jar|csv"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False

        path = parsed.path.lower()
        query = (parsed.query or "").lower()

        if any(k in query for k in [
            "action=diff",
            "format=txt",
            "precision=second",
            "version=",
            "timeline",
            "from="
        ]):
            return False

        if "/events/" in path:
            return False

        if "/event/" in path:
            return False

        if re.search(r"/\d{4}-\d{2}$", path):
            return False

        # 4) Events/calendar traps (minimal)
        if "/events/tag/" in path or "/events/day/" in path:
            return False
        if "/events/week" in path or "/events/month" in path or "/events/list" in path:
            return False
        if "ical=1" in query or "tribe_" in query or "tribe-bar-date" in query:
            return False

        # 5) DokuWiki trap
        if path.startswith("/doku.php"):
            return False
        if "/lib/exe/fetch.php" in path:
            return False

        # 6) Login pages
        if path.endswith("/wp-login.php"):
            return False

        # 7) Basic depth / repetition
        segments = [s for s in path.split("/") if s]
        if len(segments) > 12:
            return False
        if any(count > 3 for count in Counter(segments).values()):
            return False

        return True

    except TypeError:
        print("TypeError for ", parsed)
        return False


def _remove_junk_tags(soup: BeautifulSoup):
    """Remove tags that are not useful for text analytics."""
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()


def _tokenize(text: str):
    """Tokenize visible text into lowercase words/numbers."""
    text = text.lower()
    return [t for t in WORD_RE.findall(text) if len(t) >= 2]


def _has_repeated_segments(segments):
    """Detect repeating patterns in path segments."""
    if len(segments) < 6:
        return False
    counts = Counter(segments)
    if max(counts.values()) >= 4:
        return True
    streak = 1
    for i in range(1, len(segments)):
        if segments[i] == segments[i - 1]:
            streak += 1
            if streak >= 3:
                return True
        else:
            streak = 1
    return False


def _path_signature(host, path, query_dict):
    segs = [s for s in path.split("/") if s]
    norm_segs = []
    for s in segs[:10]:
        if re.fullmatch(r"\d+", s):
            norm_segs.append("{num}")
        elif re.fullmatch(r"[0-9a-f]{8,}", s, flags = re.IGNORECASE):
            norm_segs.append("{hex}")
        else:
            norm_segs.append(s.lower())

    qkeys = sorted(k.lower() for k in query_dict.keys())[:8]

    return host + "/" + "/".join(norm_segs) + "?" + "&".join(qkeys)
