import csv
from newsapi import NewsApiClient
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
API_KEY = os.getenv("NEWS_API_KEY")
newsapi = NewsApiClient(api_key=API_KEY)

today = datetime.datetime.today()
dateDelay = today + datetime.timedelta(days=-3)
todayString = today.strftime("%Y-%m-%d")
dateDelayString = dateDelay.strftime("%Y-%m-%d")

with open("sources.csv", 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)
    sourceList = []
    for row in reader:
        sourceList.append(row[0])
    
    topArticles = newsapi.get_everything(sources=", ".join(sourceList), 
                                        sort_by="popularity", language="en", 
                                        page_size=100, 
                                        from_param=dateDelayString, 
                                        to=todayString)["articles"]

with open("articles.csv", "w") as file:
    writer = csv.writer(file)
    
    writer.writerow(["source", "author", "title", "url","publishedAt"])
    for article in topArticles:
        writer.writerow([article["source"]["name"], 
                         article["author"], 
                         article["title"], 
                         article["url"],
                         article["publishedAt"]])