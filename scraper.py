import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
from collections import Counter


# O(1) efficient lookup to check if we've already processed a url with a set
unique_urls = set() 
page_counter = 0
stats = {
    "longest_page": ["", 0],  # (url, word_count) 
    "word_freq": Counter(),    
    "subdomains": Counter()  
}

# lifted from provided link of common English stop words
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by",
    "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't",
    "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have",
    "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself",
    "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into",
    "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my",
    "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
    "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's",
    "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs",
    "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't",
    "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's",
    "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't",
    "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
}


MAX_CONTENT_SIZE = 1_000_000  
MIN_WORD_COUNT   = 75         # skip links with low information content
MAX_URL_DEPTH    = 10         # >10 path segments could be a url trap
MAX_QUERY_PARAMS = 5          # more than 5 query params often means a dynamically-generated page


def save_report_progress():
    # writes current stats to disk every 50 pages so progress isn't lost if the crawler crashes
    with open("crawler_report_stats.txt", "w") as file:
        file.write("[CRAWLER REPORT STATS]\n\n")

        file.write(f"Total Unique URLs: {len(unique_urls)}\n\n")
        file.write(f"Longest Page: {stats['longest_page'][0]}\n")
        file.write(f"Word Count: {stats['longest_page'][1]}\n\n")

        file.write("Top 50 Common Words:\n")
        count = 0
        for word, freq in stats["word_freq"].most_common(50):
            count += 1
            file.write(f"{count}. {word}: {freq}\n")
        file.write("\n")

        file.write("Subdomains Found:\n")
        # sorting alphabetically using sorted()
        for subdomain in sorted(stats["subdomains"].keys()):
            file.write(f"{subdomain}, {stats['subdomains'][subdomain]}\n")


def process_statistics(url, soup):
    global page_counter
    # urldefrag removes fragment that jumps within the page (page content not affected)
    defragmented_url, _ = urldefrag(url)
    if defragmented_url in unique_urls:
        return  # already processed
    unique_urls.add(defragmented_url)

    parsed = urlparse(defragmented_url)
    if parsed.netloc.endswith(".uci.edu"):
        stats["subdomains"][parsed.netloc] += 1

    # counting pages that are too big/small, etc but not processing
    if soup is None:
        page_counter += 1
        if page_counter % 50 == 0:
            save_report_progress()
        return

    # use body text only — soup.get_text() includes <head> content (Word XML metadata,
    # style names, etc.) which inflates counts on pages like cs224 saved as Word HTML
    body = soup.body or soup
    words = re.findall(r'[a-zA-Z]+', body.get_text().lower())

    word_count = len(words)
    if word_count > stats["longest_page"][1]:
        stats["longest_page"] = [defragmented_url, word_count]

    filtered_words = [w for w in words if w not in STOP_WORDS and len(w) > 1]
    stats["word_freq"].update(filtered_words)  # counter.update adds counts, doesn't replace

    page_counter += 1
    if page_counter % 50 == 0:
        save_report_progress()

def scraper(url, resp):
    # returns a list of valid urls to add to the frontier.
    soup = None

    # check if request succeeded and content is present before parsing 
    if resp.status == 200 and resp.raw_response and resp.raw_response.content:
        if len(resp.raw_response.content) <= MAX_CONTENT_SIZE:
            soup = BeautifulSoup(resp.raw_response.content, "lxml")
            # remove script/style blocks so their source code isn't tokenized as words
            for tag in soup(["script", "style"]):
                tag.decompose()
        process_statistics(url, soup)
    links = extract_next_links(url, soup)
    return [link for link in links if is_valid(link)]



def extract_next_links(url, soup):
    # returns all absolute, defragmented hyperlinks found on the page.
    # returns [] for error responses, oversized pages, or low-info pages.
    if soup is None:
        return []

    try:
        # low-information pages (stubs, empty calendar entries, etc.) often link to
        # hundreds of similar low-info pages — returning [] here stops the chain
        tokenized = re.findall(r'[a-zA-Z]+', (soup.body or soup).get_text().lower())
        if len(tokenized) < MIN_WORD_COUNT:
            return []

        extracted = []
        for link in soup.find_all('a', href=True):  # href=True skips <a> tags with no href
            href = link.get('href').strip()

            # skip non-http schemes and bare fragment links (#section)
            if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue

            # urljoin resolves relative paths (/about -> https://ics.uci.edu/about)
            full_url = urljoin(url, href)

            # strip fragment — http://x.com/page#section becomes http://x.com/page
            clean_url, _ = urldefrag(full_url)
            extracted.append(clean_url)

        return extracted

    except Exception as e:
        print(f"Error processing {url}: {e}")
        return []


def is_valid(url):
    # returns true if the url should be crawled before it enters the frontier
    try:
        parsed = urlparse(url)

        # the cache server rejects anything that isn't http or https (status 603)
        if parsed.scheme not in {"http", "https"}:
            return False

        # and == d handles the bare root domain (ics.uci.edu) without the dot check falsely rejecting it
        valid_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        if not any(parsed.netloc == d or parsed.netloc.endswith("." + d) for d in valid_domains):
            return False

        # --- trap detection ---

        # path depth: split on "/" and ignore empty strings from leading/trailing slashes
        path_parts = [p for p in parsed.path.split("/") if p]
        if len(path_parts) > MAX_URL_DEPTH:
            return False  # e.g. /a/b/c/a/b/c/a/b/c/d is almost certainly a trap

        # repeating segments: if the same folder name appears 3+ times the url is looping
        # counter({...}) maps each segment to its count; any(...) short-circuits on first hit
        segment_counts = Counter(path_parts)
        if any(count >= 3 for count in segment_counts.values()):
            return False

        # excess query params: legitimate pages rarely need more than 5;
        # beyond that it's usually a dynamically-generated page explosion
        if parsed.query and len(parsed.query.split("&")) > MAX_QUERY_PARAMS:
            return False

        # block query params that generate duplicate or binary content (e.g. trac wikis)
        # parse_qs splits "version=1&action=diff" into {"version": [...], "action": [...]}
        # "precision" covers timeline?from=...&precision=second trap
        # "format"/"do" removed — too many legitimate pages use these params
        # tribe-bar-date/ical/outlook-ical/eventDisplay: WordPress "The Events Calendar" plugin trap
        #   — tribe-bar-date increments one day at a time, ical/outlook-ical add format variants
        blocked_params = {
            "version", "action", "rev", "diff", "precision",
            "tribe-bar-date", "ical", "outlook-ical", "eventDisplay",
        }
        if blocked_params & parse_qs(parsed.query).keys():
            return False

        # block trac attachment paths — these serve raw binary files (zip, war, svg, etc.)
        # the extension filter below misses them because the path ends in a wiki slug, not a file ext
        if re.search(r"/(zip|raw)-attachment/", parsed.path):
            return False

        # block trac timeline — even without query params it's a low-info navigation page,
        # and with any query it generates a unique url per timestamp
        if re.search(r"/timeline", parsed.path):
            return False

        # calendar trap: /calendar/ and /cal/ generate infinite next/prev-month navigation urls.
        # /events/ is intentionally excluded — seminar and event listing pages are legitimate content.
        if re.search(r"/(calendar|cal)/", parsed.path, re.IGNORECASE):
            return False

        # day-view calendar trap: WordPress Events Calendar generates /day/YYYY-MM-DD,
        # /month/YYYY-MM, etc. — one URL per day/month counting back through history.
        # also catches generic date-stamped paths like /talks/day/2021-01-13.
        if re.search(r"/(day|month|week)/\d{4}-\d{2}", parsed.path):
            return False

        # date-as-path-segment trap: catches /events/2025-10-16 and /events/category/.../1982-07
        # where a YYYY-MM-DD or YYYY-MM date is a standalone path segment
        if re.search(r"/\d{4}-\d{2}(-\d{2})?(/|$)", parsed.path):
            return False

        # file extension filter: skip binary/media/document files — no text to index
        # re.match checks from the start of the string; $ anchors to the end of the path
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|war|svg"
            + r"|sql|php|json|xml|java|sh|ppsx|mpg)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise
