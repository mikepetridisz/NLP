#### Description:
[Hacker News](https://news.ycombinator.com) scraper. 
Given a BeautifulSoup nested data structure, parses the data and selects the following fields: ***Title, URL, Author, Comments, Points, Rank***,  creates columns based on these names and saves the scraped data in a Spark DataFrame. Scrapes data from user-defined number of stories.  URLs posted by Hacker News users are pointing to different websites - scrapes these websites' data, then creates an additional column ***Keywords*** and implements Natural Language Procerssing to detect keywords from the text. Implements an execution scheduler that automatically re-runs the program. Creates an alerting system, which alerts every time a "keyword" e.g "AI" gets more popular.

**Requirements:**
 - **Currently in Python 3.7**
 - **PyCharm IDE**
 - **Apache Spark**
 - **Cloud (AWS):**  Streams/writes the DataFrame directly to the defined S3 bucket without having to save the CSV  to local disk. 

[![Screen-Shot-2019-08-13-at-1-40-30-AM.png](https://i.postimg.cc/d3L30qLW/Screen-Shot-2019-08-13-at-1-40-30-AM.png)](https://postimg.cc/MfJqPJ0R)
