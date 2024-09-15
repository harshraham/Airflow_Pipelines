import pandas as pd
from requests_html import HTMLSession
import requests
from datetime import datetime
import os

# codecs provides access to the internal Python codec registry
import codecs

# This is to translate the text from Hindi to English
from deep_translator import GoogleTranslator

# This is to analyse the sentiment of text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def crawl_page(url, sleep=1, scrolldown=2, timeout=10):
    session = HTMLSession()
    r = session.get(url)
    print(f"crawling page {url}")
    r.html.render(sleep=1, scrolldown=2, timeout=10)
    session.close()
    return r

def find_top_5_stories_finshots(ticker):
    print(f"starting scraping for {ticker} finshots")
    url = f"https://backend.finshots.in/backend/search/?q={ticker}"
    resp = requests.get(url)
    results = resp.json()['matches']
    print(results.text)
    results = sorted(results, key=lambda x: x['published_date'], reverse=True)[:5]
    stories = {}
    for i in results:
        article, sentiment = get_article_sentiment_finshots(i['post_url'])
        stories[i['title']] = {"url": i['post_url'], "article": article, "sentiment_score": sentiment}
    print(stories)
    return pd.DataFrame(stories).transpose().reset_index().rename(columns={'index':'title'}).reset_index()


def get_article_sentiment_finshots(absolute_url):
    r = crawl_page(absolute_url, 1, 2, 10)
    article = r.html.find('article', first=True)
    title = article.find('h1', first=True).text
    article_text = article.find('div.post-content', first=True).text
    analyzer = SentimentIntensityAnalyzer()
    sentiment_score = (analyzer.polarity_scores((title+' '+article_text).replace('\n',' ').replace('\xa0',' '))['compound'] + 1) / 2
    return title + '\n' + article_text, sentiment_score


def find_top_5_stories_your_story(ticker):
    print(f"starting scraping for {ticker} yourstory")
    url = "https://yourstory.com"
    r = crawl_page(url + "/search?q=" + ticker + "&page=1", 1, 2, 10)
    results = r.html.find('.container-results', first=True).find('a')
    print(results)
    stories = {}

    for link in results:
        link_val = link.absolute_links.pop()
        title = link.text
        if title not in stories and link_val.split('/')[3] not in ('video', 'videos') and link.attrs['class'][0] != 'ais-Pagination-link' and link.text != '':
            article, sentiment = get_article_sentiment_your_story(link_val)
            if article:
                # print(title,link_val)
                stories[title] = {"url": link_val, "article": article, "sentiment_score": sentiment}
                if len(stories) == 5: break
    print(stories)
    return pd.DataFrame(stories).transpose().reset_index().rename(columns={'index':'title'}).reset_index()


def translate_hindi_to_english(text):
    text=text.replace('|','.')
    split_text=text.split('.')
    translated_chunks=[]
    for i in range(0,len(split_text),10):
        translated_chunks.append(GoogleTranslator(source='hindi', target='en').translate('.'.join(split_text[i:i+10])))
    return '.'.join(translated_chunks)

def get_article_sentiment_your_story(absolute_url):
    r = crawl_page(absolute_url, 1, 2, 10)
    article = r.html.find('article', first=True)
    if '404: Page Not found' in article.text:
        return None,None
    title = article.find('h1', first=True).text
    header = article.find('h2', first=True).text
    article_text = ' '.join([i.text for i in article.find('#article_container', first=True).find('p')])
    analyzer = SentimentIntensityAnalyzer()
    if absolute_url.split('/')[3] == 'hindi':
        try:
            sentiment_score = (analyzer.polarity_scores(translate_hindi_to_english((title+' '+header+' '+article_text).replace('\n',' ').replace('\xa0',' ')))['compound'] + 1) / 2
        except:
            print('google translate failed->',(title+' '+header+' '+article_text))
            sentiment_score=0
    else:
        sentiment_score = (analyzer.polarity_scores((title+' '+header+' '+article_text).replace('\n',' ').replace('\xa0',' '))['compound'] + 1) / 2
    return title + '\n' + header + '\n' + article_text, sentiment_score


def get_top_5_stories_hdfc():
    print("starting scraping for hdfc")
    date=datetime.today().strftime('%Y-%m-%d')
    ticker='HDFC'
    outdir_your_story = f"/opt/airflow/output/source=your_story/ticker={ticker.replace(' ','_')}/date={date}/"
    outdir_finshots=f"/opt/airflow/output/source=finshots/ticker={ticker.replace(' ','_')}/date={date}/"
    if not os.path.exists(outdir_your_story):
        os.mkdir(outdir_your_story)

    if not os.path.exists(outdir_finshots):
        os.mkdir(outdir_finshots)

    df_your_story=find_top_5_stories_your_story(ticker)
    df_your_story.to_csv(outdir_your_story+'data.csv',index=False)
    df_finshots=find_top_5_stories_finshots(ticker)
    df_finshots.to_csv(outdir_finshots+'data.csv',index=False)

def get_top_5_stories_tata_motors():
    print("starting scraping for tata motors")
    date=datetime.today().strftime('%Y-%m-%d')
    ticker='Tata Motors'
    outdir_your_story = f"/opt/airflow/output/source=your_story/ticker={ticker.replace(' ','_')}/date={date}/"
    outdir_finshots=f"/opt/airflow/output/source=finshots/ticker={ticker.replace(' ','_')}/date={date}/"
    if not os.path.exists(outdir_your_story):
        os.mkdir(outdir_your_story)

    if not os.path.exists(outdir_finshots):
        os.mkdir(outdir_finshots)


    df_your_story=find_top_5_stories_your_story(ticker)
    df_your_story.to_csv(outdir_your_story+'data.csv',index=False)
    df_finshots=find_top_5_stories_finshots(ticker)
    df_finshots.to_csv(outdir_finshots+'data.csv',index=False)