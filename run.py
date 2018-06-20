#!/usr/bin/python
# filename: run.py
import re
import numpy as np

from crawler import Crawler, CrawlerCache

if __name__ == "__main__":
  # Using SQLite as a cache to avoid pulling twice
  crawler = Crawler(CrawlerCache('crawler.db'))
  root_re = re.compile('[.]*/books/download/[.]*').match
  for i in np.arange(0, 18201,20):
    id = str(i)
    print(i)
    crawler.crawl('https://www.smashwords.com/books/category/1/newest/0/free/medium/' + id, no_cache=root_re)

