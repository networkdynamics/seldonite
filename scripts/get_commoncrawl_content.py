import hashlib
import json
import logging
import os

from newsplease.crawler import commoncrawl_crawler


def __get_pretty_filepath(path, article):
    """
    Pretty might be an euphemism, but this function tries to avoid too long filenames, while keeping some structure.
    :param path:
    :param name:
    :return:
    """
    short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
    sub_dir = article.source_domain
    final_path = os.path.join(path, sub_dir)
    os.makedirs(final_path, exist_ok=True)
    return os.path.join(final_path, short_filename + '.json')


def on_valid_article_extracted(article):
    """
    This function will be invoked for each article that was extracted successfully from the archived data and that
    satisfies the filter criteria.
    :param article:
    :return:
    """
    this_dir = os.path.dirname(os.path.abspath(__file__))
    my_local_download_dir_article = os.path.join(this_dir, "..", 'data/cc_download_articles/')

    # do whatever you need to do with the article (e.g., save it to disk, store it in ElasticSearch, etc.)
    with open(__get_pretty_filepath(my_local_download_dir_article, article), 'w', encoding='utf-8') as outfile:
        json.dump(article.__dict__, outfile, default=str, indent=4, sort_keys=True, ensure_ascii=False)


def main():

    ############ YOUR CONFIG ############
    # download dir for warc files
    this_dir = os.path.dirname(os.path.abspath(__file__))
    my_local_download_dir_warc = os.path.join(this_dir, "..", 'data/cc_download_warc/')
    # download dir for articles
    
    # hosts (if None or empty list, any host is OK)
    my_filter_valid_hosts = ['cbc.ca']  # example: ['elrancaguino.cl']
    # start date (if None, any date is OK as start date), as datetime
    my_filter_start_date = None  # datetime.datetime(2016, 1, 1)
    # end date (if None, any date is OK as end date), as datetime
    my_filter_end_date = None  # datetime.datetime(2016, 12, 31)
    # if date filtering is strict and news-please could not detect the date of an article, the article will be discarded
    my_filter_strict_date = True
    # if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
    # again. Note that there is no check whether the file has been downloaded completely or is valid!
    my_reuse_previously_downloaded_files = True
    # continue after error
    my_continue_after_error = True
    # show the progress of downloading the WARC files
    my_show_download_progress = True
    # log_level
    my_log_level = logging.INFO
    # number of extraction processes
    my_number_of_extraction_processes = 1
    # if True, the WARC file will be deleted after all articles have been extracted from it
    my_delete_warc_after_extraction = True
    # if True, will continue extraction from the latest fully downloaded but not fully extracted WARC files and then
    # crawling new WARC files. This assumes that the filter criteria have not been changed since the previous run!
    my_continue_process = True
    # if True, will crawl and extract main image of each article. Note that the WARC files
    # do not contain any images, so that news-please will crawl the current image from
    # the articles online webpage, if this option is enabled.
    my_fetch_images = False
    ############ END YOUR CONFIG #########

    commoncrawl_crawler.crawl_from_commoncrawl(on_valid_article_extracted,
                                               valid_hosts=my_filter_valid_hosts,
                                               start_date=my_filter_start_date,
                                               end_date=my_filter_end_date,
                                               strict_date=my_filter_strict_date,
                                               reuse_previously_downloaded_files=my_reuse_previously_downloaded_files,
                                               local_download_dir_warc=my_local_download_dir_warc,
                                               continue_after_error=my_continue_after_error,
                                               show_download_progress=my_show_download_progress,
                                               number_of_extraction_processes=my_number_of_extraction_processes,
                                               log_level=my_log_level,
                                               delete_warc_after_extraction=my_delete_warc_after_extraction,
                                               continue_process=my_continue_process,
                                               fetch_images=my_fetch_images)

if __name__ == '__main__':
    main()