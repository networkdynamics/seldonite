from urllib.parse import urlparse

from flashgeotext.geotext import GeoText
import langdetect

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

def get_countries(text):
    geotext = GeoText()
    places = geotext.extract(input_text=text)
        
    return list(places['countries'].keys())

def get_language(text):
    try:
        return langdetect.detect(text)
    except Exception as err:
        raise Exception(f"Recieved exception: {err}, for input: {text}")

