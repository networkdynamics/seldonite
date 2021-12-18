from urllib.parse import urlparse
from seldonite.filters import political

def contains_keywords(article, keywords):
    if any(keyword in article.title for keyword in keywords):
        return True
    elif any(keyword in article.text for keyword in keywords):
        return True
    else:
        return False

def check_url_from_sites(url, sites):
    domain = urlparse(url).hostname
    return any(f".{site}" in domain or f"/{site}" in domain for site in sites)