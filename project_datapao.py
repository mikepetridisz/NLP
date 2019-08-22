""" Datapao Project"""

import boto3
import findspark
findspark.init()
import smart_open
import io
import csv
import argparse
import re
import urllib
import urllib.request
import urllib.parse
import urllib.error
from io import StringIO
from pyspark import SparkFiles

# https://news.ycombinator.com/robots.txt | Crawl Delay: 30 | Robots.txt
from time import sleep

from urllib.error import URLError
import os
import urllib.request
from contextlib import closing
from itertools import groupby
from math import ceil
from fake_useragent import UserAgent
import pandas as pd
import validators
from bs4 import BeautifulSoup
from bs4.element import Comment
from nltk.corpus import stopwords
from requests import get
from requests.exceptions import RequestException
from summa import keywords
import nltk
from nltk.stem import PorterStemmer, WordNetLemmatizer
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


MAX_NUM_POSTS = 100


class HackerNewsScraper:
    # Global variable
    URL = 'https://news.ycombinator.com/news'

    # To automatically set these variables I used __init__ self
    # Self takes the instance, posts -> argument
    def __init__(self, posts):
        self._total_posts = posts

        # If we want to scrape e.g 60 stories, then 60/30 = 2 => 2 pages will be scraped.
        self._total_pages = int(ceil(posts / 30))  # ceil == 46,47 ..59, 60 is still on page 2.
        # empty list creation
        self._stories = []

    def scrape_stories(self):
        """
        Default 30 stories / page. This ensures enough HTML data (pages) are fetched.
        """
        page = 1

        # Visit sufficient number of pages
        while page <= self._total_pages:
            url = '{}?p={}'.format(self.URL, page)  # str.format() https://news.ycombinator.com/?p=1/2/3/4/5/etc..
            html = get_html(url)
            self.parse_stories(html)
            page += 1

    def parse_stories(self, html):
        """
        Beautifulsoup nested data structure
        -> parse_stories(html) parses the data and selects the following fields:
        title, url as uri, author, comments, points, rank and keywords.
        Saves the data in dictionary form in self._stories.
        """

        for storytext, subtext in zip(html.find_all('tr', {'class': 'athing'}),
                                      html.find_all('td', {'class': 'subtext'})):

            storylink = storytext.find_all('a', {'class': 'storylink'})
            sublink = subtext.select('a')

            # data -> save -> dictionary
            TITLE = storylink[0].text.strip()
            URI = storylink[0]['href']
            AUTHOR = sublink[0].text
            COMMENTS = sublink[-1].text
            POINTS = subtext.select('span')[0].text
            RANK = storytext.select('span.rank')[0].text.strip('.')

            # Ensures only text gets scraped
            def tag_visible(element):
                if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
                    return False
                if isinstance(element, Comment):
                    return False
                return True

            #  Text only selection
            def text_from_html(body):
                soup = BeautifulSoup(body, 'html.parser')
                texts = soup.findAll(text=True)
                visible_texts = filter(tag_visible, texts)
                return u" ".join(t.strip() for t in visible_texts)

            ''' 
            NLP implementation
            '''

            # User Agent - Resolves the HTTP Error 403
            ua = UserAgent()
            req = urllib.request.Request(URI)
            req.add_header('User-Agent', ua.chrome)
            html = urllib.request.urlopen(req).read()

            # Optional User Agents if the default fails
            '''
            hdr = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                'Accept-Encoding': 'none',
                'Accept-Language': 'en-US,en;q=0.8',
                'Connection': 'keep-alive'}
            '''

            try:
                html = urllib.request.urlopen(req).read()
            except URLError as e:
                if hasattr(e, 'reason'):
                    print('We failed to reach a server.')
                    print('Reason: ', e.reason)
                elif hasattr(e, 'code'):
                    print('The server couldn\'t fulfill the request.')
                    print('Error code: ', e.code)
            else:
                fulltext = (text_from_html(html))
            # English stop words implementation
            english_stopwords = stopwords.words('english')
            # Lower-case characters only
            fulltext = fulltext.lower()
            # Regex
            document = re.sub("<!--?.*?-->", "", fulltext)
            document = re.sub("(\\d|\\W)+", " ", fulltext)
            # Text gets tokenized
            words = (nltk.wordpunct_tokenize(document))
            # Stop words get taken out
            document = [w for w in words if w.lower() not in english_stopwords]
            # Stemming
            stemmer = PorterStemmer()
            document = list(map(stemmer.stem, document))
            # Lemmatizing
            lemmatizer = WordNetLemmatizer()
            document = ' '.join([lemmatizer.lemmatize(w) for w in document])
            # Selects keywords
            kwords = keywords.keywords((str(document)), words=(5), ratio=0.2, language='english')
            # Makes sure there are no duplicate words
            kwords = ' '.join(item[0] for item in groupby(kwords.split()))

            KEYWORDS = kwords


            story = {
                'title': TITLE,
                'uri': URI,
                'author': AUTHOR,
                'points': POINTS,
                'comments': COMMENTS,
                'rank': RANK,
                'keywords': KEYWORDS
            }

            # Make sure data meets requirements
            story = validate_story(story)

            # self._stories is an array of dictionaries -> saves the requested number of stories
            self._stories.append(story)

            # If required number of stories met -> stop parsing
            if len(self._stories) >= self._total_posts:
                return

    def print_stories(self):
        """
        Outputs the stories from list of dictionary format to pandas DataFrame
        """

        pdf = pd.DataFrame(data=(self._stories),
                           columns=['title', 'uri', 'author', 'points', 'comments', 'rank', 'keywords'])

        # Apache Spark schema
        mySchema = StructType([
            StructField("title", StringType(), True),
            StructField("uri", StringType(), True),
            StructField("author", StringType(), True),
            StructField("points", IntegerType(), True),
            StructField("comments", IntegerType(), True),
            StructField("rank", IntegerType(), True),
            StructField("keywords", StringType(), True)
        ])
        # Based on the defined schema, converts pandas DataFrame to Spark DataFrame
        df = spark.createDataFrame(pdf, schema=mySchema)
        # If True -> no abbreviation, if false -> Abbreviates DataFrame
        df.show(100, False)
        # Storage - CSV (data is relatively small, it can be JSON/Parquet etc as well)

        #Save to S3
        # df.write.option("header", True).csv("s3://hackernews-project/dpao/")
        # df.repartition(1).write.mode("overwrite").format("csv").option("header", "true").csv("s3://hackernews-project/dpao.csv")
        
        '''
        Found a way to stream/write directly to AWS S3 without having to save to disk plus automatized the upload.
        This way, as the execution scheduler re-executes the code, a new file gets uploaded to S3 automatically.
        '''
        df = io.StringIO()
        with smart_open.smart_open('s3://hackernews-project/dpao/foo.csv', 'wb') as fout:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            fout.write(f.getvalue())

            for row in df:
                f.seek(0)
                f.truncate(0)
                writer.writerow(row)
                fout.write(f.getvalue())
        f.close()
        

def get_html(url):
    """
    Runs the HTML data through BeautifulSoup to get a BeautifulSoup object
    """
    response = get_response(url)

    if response is not None:
        html = BeautifulSoup(response, 'html.parser')

        return html


def validate_story(story):
    """
    Ensures that all the story data is valid according to the task.
    Will return valid data for each field.
    """
    story['title'] = story['title'][:256]
    if not valid_title(story['title']):
        story['title'] = 'Valid title not found'

    story['author'] = story['author'][:256]
    if not valid_author(story['author']):
        story['author'] = 'Valid author not found'

    if not valid_url(story['uri']):
        story['uri'] = 'Valid URI not found'

    story['comments'] = validate_number(story['comments'])
    story['points'] = validate_number(story['points'])
    story['rank'] = validate_number(story['rank'])

    return story


def valid_title(title):
    """
    Ensures that title is non empty string with <= 256 characters
    """
    return len(title) <= 256 and title


def valid_author(author):
    """
    Solved the issue of not finding an author by checking the fetched data with HN username rules.
    """
    # Hacker news username doesnt support whitespace
    if author.find(' ') > -1:
        return False
    # Ensures that author is non empty string and <= 256 characters.
    return len(author) <= 256 and author


def valid_url(url):
    """
    To be able to find the scraped stories, we need the URL.
    If data is not a valid URL, returns False.
    """
    if validators.url(url):
        return True
    return False


def validate_number(numString):
    """
    Will make sure that the returned number is an int.
    Will strip any non digits from the input and return the first number.
    """
    # If not found, 'time since posted' would replace points for example
    if numString.find('ago') > -1:
        return 0

    digits = [int(s) for s in numString.split() if s.isdigit()]

    if len(digits) > 0:
        return digits[0]
    return 0


def get_response(url):
    try:
        #  Contextlib [closing] closes thing upon completion of the block.
        """
           Attempts to get the content at `url` by making an HTTP GET request.
           If the content-type of response is some kind of HTML/XML, return the
           text content, otherwise return None
        """
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        #  In Python 3.x str(e) should be able to convert any Exception to a string, even if it contains Unicode characters.
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None


def is_good_response(resp):
    """
    Returns True if the response == HTML
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    """
    Logs the errors -> printing them out
    """
    print(e)


def validate_input(arg, arg_max):
    """
    Validates the user input. Currently only less than or equal to 100 posts.
    """
    error_msg = 'Posts cannot exceed {}'.format(arg_max)
    if arg > arg_max:
        raise argparse.ArgumentTypeError(error_msg)


def parse_arguments():
    """
    Parses the argument input from the user. Modify default = x
    to scrape x number of stories. Max 100
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--posts', '-p', metavar='n', type=int, default=1, help='number of posts (max 100)')
    args = parser.parse_args()

    validate_input(args.posts, MAX_NUM_POSTS)

    return args.posts


# Execution Scheduler, to execute code time to time (user-defined)
'''
def execution_scheduler():
    """
    Re-runs the code every 10 seconds
    """
    schedulerl= BlockingSchedulem(k
    scheduler.add_job(main, 'interval', seconds=10, max_instances=5)
    scheduler.start()
'''


def main():
    """
    If user input is valid, will create a scraper and fetch requests number of posts and print them out.
    """
    # Executes code following the try statement as a “normal” part of the program
    try:
        posts = parse_arguments()
        hnews_scraper = HackerNewsScraper(posts)
        hnews_scraper.scrape_stories()
        hnews_scraper.print_stories()
        # Commented out the execution schedculer due to the HTTP Error 403
        ''' execution_scheduler() '''



    # the program 's response to any exceptions in the preceding try clause.
    except argparse.ArgumentTypeError as ex:
        log_error(ex)


if __name__ == '__main__':
    main()
