# Importing Libraries
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import pandas as pd
import nltk
import warnings

warnings.filterwarnings('ignore')

nltk.downloader.download('vader_lexicon')
from newspaper import Article
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pymongo
import os
from dotenv import load_dotenv


def get_database():
    client = pymongo.MongoClient(os.getenv('DB_CON_STR'))
    db = client.graham
    return db

load_dotenv()
print(os.getenv('DB_CON_STR'))
# We are scraping news articles from this URL
invest_url = "https://www.investing.com/equities/{}-news"

# BASE_PATH = "D:\Internship-WFC\TEAM-A10\ML\Data\\"

# Importing data
df = pd.read_excel('name.xlsx', index_col=0)

# creating Dataframe
article_sentiments = pd.DataFrame({'ticker': [], 'title': [], 'neg': [], 'neu': [], 'pos': [], 'compound': []})
counter = 0


# sentiment_scores = {}


try:
    db = get_database()
    collection_name = db[os.getenv('DB_COLLECTION')]
except Exception as e:
    print(e)
    print("Error in connecting to database")
    exit()

# Checking For First 10 companies
for ticker, symbol in df[['Name', 'Ticker']].itertuples(index=False):
    try:
        insert_doc = {'ticker': symbol}

        # List to store links
        all_links = []

        # List to store Text
        all_data = []

        url = invest_url.format(ticker)
        req = Request(url=url, headers={'user-agent': 'my-app'})
        response = urlopen(req)
        html = BeautifulSoup(response, 'html.parser')

        anchors = html.findAll('a', class_='title')
        data = html.findAll(class_='title')

        for link in anchors[6:7]:
            link = "https://www.investing.com" + link.get('href')
            all_links.append(link)

        # Loop over all the articles
        # Creating DataFrame
        counter = 0
        for link in all_links:

            article = Article(link)
            article.download()

            try:
                article.parse()
                text = article.text
            except:
                continue

            # Initialise sentiment analyser
            sid = SentimentIntensityAnalyzer()
            # Get positive, negative, neutral and compound scores
            polarity = sid.polarity_scores(text)

            tmpdic = {'ticker': ticker, 'title': article.title, 'body_text': article.text, 'symbol': symbol}
            # Update ticker with the new entry polarity
            tmpdic.update(polarity)
            # tmpdic now has all keys and values needed to populate the DataFrame
            article_sentiments = article_sentiments.append(pd.DataFrame(tmpdic, index=[0]))
            article_sentiments.reset_index(drop=True, inplace=True)

        counter += 1
        insert_doc['index'] = article_sentiments['compound'].mean() * 100
        print("Sentiment score for {} is {}%".format(ticker, article_sentiments['compound'].mean() * 100))
        collection_name.update_one({'ticker':symbol},{'$set': {'index': article_sentiments['compound'].mean() * 100}},upsert=True)
    except:
        print(f'failed for {symbol}')

