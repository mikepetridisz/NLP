## Datapao 
#### Project Description:
Build a [Hacker News](https://news.ycombinator.com) scraper. Given a BeautifulSoup nested data structure, parse the data and select the desired fields. After getting the ***Title, URL, Author, Comments, Points, Rank*** fields,  create columns based on these names and save scraped data in a Spark DataFrame.  Scrape text from adjustable number of stories.  URLs posted by Hacker News users are pointing to different websites - scrape their texts as well, then create an additional column ***Keywords*** and use NLP to detect keywords from that text.  (Note: There are websites that use Javascript to render. Contents that are rendered with JS can't be fetched using `requests`, it should be handled). Implement an execution scheduler that automatically re-runs the program and saves the latest data. Once the data is in the DataFrame, create reports and save them. Create an alerting system, which alerts every time a "keyword" e.g "AI" gets more and more attention. 

**Requirements:**
 - Cloud (AWS) implementation, EC2, S3
 - Written in Python 3.5 or 3.6
 - PyCharm IDE
 - Data processing == Apache Spark

**Hacker News:**
[https://news.ycombinator.com/](https://news.ycombinator.com/)


[![Screen-Shot-2019-08-13-at-1-40-30-AM.png](https://i.postimg.cc/d3L30qLW/Screen-Shot-2019-08-13-at-1-40-30-AM.png)](https://postimg.cc/MfJqPJ0R)
