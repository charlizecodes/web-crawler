import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter

# --- global state for report ---
# a set gives O(1) lookup, so checking "have i seen this url?" is fast
unique_urls = set()
page_counter = 0
stats = {
    "longest_page": ["", 0],   # [url, word_count]
    "word_freq": Counter(),     # counter maps word -> total occurrences across all pages
    "subdomains": Counter()     # subdomain hostname -> unique page count
}

# common english words that don't carry meaning — we exclude these from word frequency
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

# tuning constants — adjust these during the test period if the crawler misbehaves
MAX_CONTENT_SIZE = 1_000_000  # 1 mb: skip parsing pages larger than this to avoid memory spikes
MIN_WORD_COUNT   = 200        # pages with fewer words are considered low-information; don't follow their links
MAX_URL_DEPTH    = 10         # more than 10 path segments is a strong signal of a url trap
MAX_QUERY_PARAMS = 5          # more than 5 query params often means a dynamically-generated trap page


def save_report_progress():
    # writes current stats to disk every 50 pages so progress isn't lost if the crawler crashes
    with open("crawler_report_stats.txt", "w") as file:
        file.write("[CRAWLER REPORT STATS]\n\n")

        file.write(f"Total Unique Pages: {len(unique_urls)}\n\n")
        file.write(f"Longest Page: {stats['longest_page'][0]}\n")
        file.write(f"Word Count: {stats['longest_page'][1]}\n\n")

        file.write("Top 50 Common Words:\n")
        for word, freq in stats["word_freq"].most_common(50):
            file.write(f"{word}: {freq}\n")
        file.write("\n")

        # spec format: "vision.ics.uci.edu, 10"
        file.write("Subdomains Found:\n")
        for sd in sorted(stats["subdomains"].keys()):
            file.write(f"{sd}, {stats['subdomains'][sd]}\n")


def scraper(url, resp):
    # entry point called by the crawler framework for every fetched page.
    # returns a list of valid urls to add to the frontier.
    links = extract_next_links(url, resp)
    if resp.status == 200 and resp.raw_response and resp.raw_response.content:
        process_statistics(url, resp)
    return [link for link in links if is_valid(link)]


def process_statistics(url, resp):
    # records data needed for the report. called once per unique url.
    global page_counter

    # urldefrag strips the fragment (#section) so http://x.com#a and http://x.com#b are the same page
    clean_url, _ = urldefrag(url)
    if clean_url in unique_urls:
        return  # already processed this url — skip to avoid double-counting
    unique_urls.add(clean_url)

    # count this url toward its subdomain now, before the size check below,
    # so large pages still appear in the subdomain report
    parsed = urlparse(clean_url)
    if parsed.netloc.endswith(".uci.edu"):
        stats["subdomains"][parsed.netloc] += 1

    # pages over 1 mb are still counted as unique urls and in subdomains,
    # but we skip their content analysis to avoid memory issues
    if len(resp.raw_response.content) > MAX_CONTENT_SIZE:
        page_counter += 1
        if page_counter % 50 == 0:
            save_report_progress()
        return

    # lxml is the fastest html parser available to beautifulsoup
    soup = BeautifulSoup(resp.raw_response.content, "lxml")

    # get_text() strips all html tags — word count is text-only per the spec
    text = soup.get_text()

    # findall with [a-zA-Z0-9]+ tokenizes by alphanumeric runs (splits on spaces, punctuation, etc.)
    words = re.findall(r'[a-zA-Z0-9]+', text.lower())

    word_count = len(words)
    if word_count > stats["longest_page"][1]:
        stats["longest_page"] = [clean_url, word_count]

    # filter stop words and single-character tokens before updating frequency counts
    filtered_words = [w for w in words if w not in STOP_WORDS and len(w) > 1]
    stats["word_freq"].update(filtered_words)  # counter.update adds counts, it doesn't replace them

    page_counter += 1
    if page_counter % 50 == 0:
        save_report_progress()


def extract_next_links(url, resp):
    # parses the page and returns all absolute, defragmented hyperlinks found on it.
    # returns [] (no links to follow) for error responses, oversized pages, or low-info pages.

    # non-200 status means the page didn't load properly — nothing to extract
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
        return list()

    content = resp.raw_response.content
    if len(content) > MAX_CONTENT_SIZE:
        return list()

    try:
        soup = BeautifulSoup(content, "lxml")

        # low-information pages (stubs, empty calendar entries, etc.) often link to
        # hundreds of similar low-info pages — returning [] here stops the chain
        text = soup.get_text()
        words = re.findall(r'[a-zA-Z]+', text.lower())
        if len(words) < MIN_WORD_COUNT:
            return list()

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
        return list()


def is_valid(url):
    # returns true if the url should be crawled. called on every link before it enters the frontier.
    try:
        parsed = urlparse(url)

        # the cache server rejects anything that isn't http or https (status 603)
        if parsed.scheme not in {"http", "https"}:
            return False

        # spec: only crawl *.ics.uci.edu, *.cs.uci.edu, *.informatics.uci.edu, *.stat.uci.edu
        # using endswith("." + d) handles subdomains (www.ics.uci.edu),
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
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise
