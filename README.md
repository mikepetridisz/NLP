## Datapao 
#### Project Description:
Build a [Hacker News](https://news.ycombinator.com) scraper. Given a BeautifulSoup nested data structure, parse the data and select the following fields: ***Title, URL, Author, Comments, Points, Rank***,  create columns based on these names and save the scraped data in a Spark DataFrame.  Scrape text from user-defined number of stories.  URLs posted by Hacker News users are pointing to different websites - scrape their data, then create an additional column ***Keywords*** and use NLP to detect keywords from that text. Implement an execution scheduler that automatically re-runs the program and saves the latest data. Once the data is in the DataFrame, create reports and save them. Create an alerting system, which alerts every time a "keyword" e.g "AI" gets more and more attention. 

**Requirements:**
 - Cloud (AWS) implementation, EC2, S3 (In Progress)
 - Written in Python 3.5 or 3.6
 - PyCharm IDE
 - Data processing == Apache Spark

**Packages/Modules Used:**
argparse, re, urllib, contextlib, closing, math, ceil
itertools, chain, groupby, pandas, validators, bs4, nltk.corpus, stopwords
requests, get, RequestException, findspark, summa, nltk,
PorterStemmer, WordNetLemmatizer, SparkContext, SparkSession

[![Screen-Shot-2019-08-13-at-1-40-30-AM.png](https://i.postimg.cc/d3L30qLW/Screen-Shot-2019-08-13-at-1-40-30-AM.png)](https://postimg.cc/MfJqPJ0R)
