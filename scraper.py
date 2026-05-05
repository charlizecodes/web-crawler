import re
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
from collections import Counter


# O(1) efficient lookup using a set to check processed urls
unique_urls = set() 
page_counter = 0
stats = {
    "longest_page": ["", 0],  # (url, word_count) 
    "common_words": Counter(),    
    "subdomains": Counter()  
}

# from provided link of common English stop words
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
MIN_WORD_COUNT   = 75         # defining low information content as word counts below 75
MAX_URL_DEPTH    = 10         # defining >10 path segments could be a url trap
MAX_QUERY_PARAMS = 5          # defining more than 5 query params often means a dynamically-generated page (trap)


def save_report_progress():
    # writes current stats to disk every 50 pages so progress isn't lost if the crawler crashes
    with open("crawler_report_stats.txt", "w") as file:
        file.write("------------CS121 Crawler Report------------\n\n")

        file.write(f"Total Unique URLs: {len(unique_urls)}\n\n")
        file.write(f"Longest Page: {stats['longest_page'][0]}\n")
        file.write(f"Word Count: {stats['longest_page'][1]}\n\n")

        file.write("Top 50 Common Words:\n")
        common_words_count = 0
        for word, freq in stats["common_words"].most_common(50):
            common_words_count += 1
            file.write(f"{common_words_count}. {word}: {freq}\n")
        file.write("\n")

        file.write("Subdomains Found:\n")
        for subdomain in sorted(stats["subdomains"].keys()):
            file.write(f"{subdomain}, {stats['subdomains'][subdomain]}\n")


def process_statistics(url, words):
    global page_counter
    defragmented_url, _ = urldefrag(url)
    if defragmented_url in unique_urls:
        return
    unique_urls.add(defragmented_url)

    parsed = urlparse(defragmented_url)
    if parsed.netloc.endswith(".uci.edu"):
        stats["subdomains"][parsed.netloc] += 1

    filtered_words = []
    word_count = 0
    for w in words:
        if w == "msonormal":  # Word HTML artifact that can leak into text nodes
            continue
        word_count += 1
        if w not in STOP_WORDS and len(w) > 1:
            filtered_words.append(w)

    if word_count > stats["longest_page"][1]:
        stats["longest_page"] = [defragmented_url, word_count]

    stats["common_words"].update(filtered_words)

    page_counter += 1
    if page_counter % 50 == 0:
        save_report_progress()


def scraper(url, resp):
    soup = None
    words = []

    if resp.status == 200 and resp.raw_response and resp.raw_response.content:
        if len(resp.raw_response.content) <= MAX_CONTENT_SIZE:
            soup = BeautifulSoup(resp.raw_response.content, "lxml")
            for tag in soup(["script", "style"]):
                tag.decompose()
            # tokenize, and then pass into process_statistics and extract_next_links
            # if lxml can't find soup.body, it returns None, so we fallback to soup to avoid an AttributeError
            body_text = (soup.body or soup).get_text(separator=" ").lower()
            words = re.findall(r'[a-zA-Z]+', body_text)
        process_statistics(url, words)
    

    if soup is None:
        return []
    #skip link extraction for non-200 status pages or pages with no content
    links = extract_next_links(url, soup, words)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, soup, words):
    try:
        #links in low information pages won't be followed
        if len(words) < MIN_WORD_COUNT:
            return []

        extracted = []
        for link in soup.find_all('a', href=True): 
            #clean up links by stripping whitespace and ignoring empty hrefs
            href = link.get('href').strip() 

            # handles empty strings, javascript, email addresses, numbers, and fragment jumps within page
            if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
                continue

            # to join paths relative to the base url
            full_url = urljoin(url, href)

            # remove useless fragments that dont affect the content 
            clean_url, _ = urldefrag(full_url)
            extracted.append(clean_url)

        return extracted

    except Exception as e:
        print(f"Skipping {url} (parse error: {e})")
        return []


def is_valid(url):
    # returns true if the url should be crawled before it enters the frontier
    try:
        spliturl = urlparse(url) # scheme://netloc/path;parameters?query#fragment.

        # only http or https schemes are valid for crawling
        if spliturl.scheme not in {"http", "https"}:
            return False

        # domain check + subdomain ( '.' + d) check to allow subdomains of the valid domains
        valid_domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        netloc = spliturl.netloc.lower()
        if not any(netloc == d or netloc.endswith("." + d) for d in valid_domains):
            return False

        # --- trap detection ---
        path  = spliturl.path
        query = spliturl.query

        # path depth: split on "/" and ignore empty strings from leading/trailing slashes
        path_parts = [p for p in path.split("/") if p]
        if len(path_parts) > MAX_URL_DEPTH:
            return False

        # skippping urls if path segments repeat excessively
        segment_counts = Counter(path_parts)
        if any(count >= 3 for count in segment_counts.values()):
            return False

        # excess query params: legitimate pages rarely need more than 5;
        # beyond that it's usually a dynamically-generated page explosion
        if query and len(query.split("&")) > MAX_QUERY_PARAMS:
            return False

        # removing crawler traps
        blocked_params = {
            "version", "action", "rev", "diff", "precision",
            "tribe-bar-date", "ical", "outlook-ical", "eventDisplay",
        }
        if blocked_params & parse_qs(query).keys():
            return False

        # avoid zip and raw-attachment paths 
        if re.search(r"/(zip|raw)-attachment/", path):
            return False

        # avoid timeline paths that generate infinite urls
        if re.search(r"/timeline", path):
            return False

        # another common calendar trap pattern with "calendar" or "cal" in the path
        if re.search(r"/(calendar|cal)/", path, re.IGNORECASE):
            return False

        # 4-digit year followed by  2 digit month (and optional day) segment is not valid
        if re.search(r"/\d{4}-\d{2}(-\d{2})?(/|$)", path):
            return False

        # filter out urls that point to non-html resources based on their file extensions
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|war|svg"
            + r"|sql|php|json|xml|java|sh|ppsx|mpg)$", path.lower())

    except TypeError:
        print("TypeError for ", spliturl)
        raise
