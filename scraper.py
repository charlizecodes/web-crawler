import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import Counter
import json

# for report data
unique_urls = set()
page_counter = 0
stats = {
    "longest_page": ["", 0],
    "word_freq": Counter(),
    "subdomains": Counter() # {subdomain: unique_url_count}
}

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



def save_report_progress():
    with open("crawler_report_stats.txt", "w") as file:
        file.write("[CRAWLER REPORT STATS]\n\n")

        file.write(f"Total Unique Pages: {len(unique_urls)}\n\n")
        file.write(f"Longest Page: {stats['longest_page'][0]}\n")
        file.write(f"Word Count: {stats['longest_page'][1]}\n\n")

        file.write("Top 50 Common Words:\n")
        for word, freq in stats["word_freq"].most_common(50):
            file.write(f"{word}: {freq}\n")
        file.write("\n")

        file.write("Subdomains Found:\n")
        for sd in sorted(stats["subdomains"].keys()):
            file.write(f"{sd}, {stats['subdomains'][sd]}\n")



def scraper(url, resp):
    links = extract_next_links(url, resp)
    if resp.status == 200 and resp.raw_response and resp.raw_response:
        process_statistics(url, resp)

    return [link for link in links if is_valid(link)]



def process_statistics(url, resp):
    global page_counter
    clean_url, _ = urldefrag(url)
    if clean_url in unique_urls:
        return
    unique_urls.add(clean_url)

    soup = BeautifulSoup(resp.raw_response.content, "lxml")
    text = soup.get_text()
    words = re.findall(r'[a-zA-Z0-9]+', text.lower())
    
    word_count = len(words)
    if word_count > stats["longest_page"][1]:
        stats["longest_page"] = [clean_url, word_count]

    filtered_words = [w for w in words if w not in STOP_WORDS and len(w) > 1]
    stats["word_freq"].update(filtered_words)

    parsed = urlparse(clean_url)
    if parsed.netloc.endswith(".uci.edu"):
        stats["subdomains"][parsed.netloc] += 1

    page_counter += 1
    if page_counter % 50 == 0:
        save_report_progress()


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scraped from resp.raw_response.content


    # check for valid response
    if resp.status != 200 or not resp.raw_response or not resp.raw_response.content:
        return list()
    
    content = resp.raw_response.content
    # check for content size to avoid memory issues
    if len(content) > 1000000:
        return list()

    try:
        # parsing content with BeautifulSoup
        soup = BeautifulSoup(content, "lxml")
        
        extracted = []
        
        # collect all hyperlinks from the page
        for link in soup.find_all('a', href=True):
            href = link.get('href').strip()
            
            # skip empty, javascript, mailto, tel, and fragment links
            if href.startswith(("javascript:", "mailto:", "tel:", "#")) or not href:
                continue
                
            # take care of relative URLs and remove fragments
            full_url = urljoin(url, href)
            clean_url, _ = urldefrag(full_url)
            
            extracted.append(clean_url)
            
        return extracted
        
    except Exception as e:
        print(f"Error processing {url}: {e}")
        return list()

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # within specified uci.edu domain check
        valid_domains = [
            ".ics.uci.edu",
            ".cs.uci.edu",
            ".informatics.uci.edu",
            ".stat.uci.edu"
        ]
        # check if the URL's network location ends with any of the valid domains
        if not any(parsed.netloc.endswith(domain) for domain in valid_domains):
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
