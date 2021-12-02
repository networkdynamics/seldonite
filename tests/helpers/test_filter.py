
import pytest

from seldonite.helpers import filter

@pytest.mark.parametrize("url, sites, is_from_sites",
    [("https://apnews.com/article/abortion-donald-trump-us-supreme-court-health-amy-coney-barrett-a3b5cf9621315e6c623dc80a790842d8", ["apnews.com"], True),
     ("https://www.kelownacapnews.com/life/the-rising-landscapes-of-painter-karel-doruyter/", ["apnews.com"], False)])
def test_check_url(url, sites, is_from_sites):
    result = filter.check_url_from_sites(url, sites)
    assert result == is_from_sites