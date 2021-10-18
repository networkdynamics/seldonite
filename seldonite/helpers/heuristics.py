"""
Helper class for testing heuristics
See here for more information https://github.com/fhamborg/news-please/wiki/crawlers-and-heuristics#heuristics
"""
import re

from newsplease.helper_classes.url_extractor import UrlExtractor

# to improve performance, regex statements are compiled only once per module
re_url_root = re.compile(r'https?://[a-z]+.')


def og_type(article):
    """
    Check if the site contains a meta-tag which contains
    property="og:type" and content="article"

    :param newspaper.Article article: 

    :return bool: True if the tag is contained.
    """
    try:
        return article.meta_data["og"]["type"] == 'article'
    except KeyError:
        return False


def linked_headlines(response, site_dict, check_self=False):
    """
    Checks how many of the headlines on the site contain links.

    :param obj response: The scrapy response
    :param dict site_dict: The site object from the JSON-File
    :param bool check_self: Check headlines/
                            headlines_containing_link_to_same_domain
                            instead of headline/headline_containing_link

    :return float: ratio headlines/headlines_containing_link
    """
    h_all = 0
    h_linked = 0
    domain = UrlExtractor.get_allowed_domain(site_dict["url"], False)

    # This regex checks, if a link containing site_domain as domain
    # is contained in a string.
    site_regex = r"href=[\"'][^\/]*\/\/(?:[^\"']*\.|)%s[\"'\/]" % domain
    for i in range(1, 7):
        for headline in response.xpath('//h%s' % i).extract():
            h_all += 1
            if "href" in headline and (
                        not check_self or re.search(site_regex, headline)
                    is not None):
                h_linked += 1

    min_headlines = 5
    if min_headlines > h_all:
        return True

    return float(h_linked) / float(h_all)

def self_linked_headlines(response, site_dict):
    """
    Checks how many of the headlines on the site contain links.

    :param obj response: The scrapy response
    :param dict site_dict: The site object from the JSON-File

    :return float: ratio headlines/headlines_containing_link_to_same_domain
    """
    return linked_headlines(response, site_dict, True)

def is_not_from_subdomain(response, site_dict):
    """
    Ensures the response's url isn't from a subdomain.

    :param obj response: The scrapy response
    :param dict site_dict: The site object from the JSON-File

    :return bool: Determines if the response's url is from a subdomain
    """

    root_url = re.sub(re_url_root, '', site_dict["url"])
    return UrlExtractor.get_allowed_domain(response.url) == root_url
