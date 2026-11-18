import csv
from newsapi import NewsApiClient
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
API_KEY = os.getenv("NEWS_API_KEY")
newsapi = NewsApiClient(api_key=API_KEY)

scienceSources = newsapi.get_sources(country="us", category="science")
businessSources = newsapi.get_sources(country="us", category="business")

requestedSources = scienceSources | businessSources
finalSources = requestedSources["sources"] #This strips the status code from the response and leaves just the sources dictionaries
# fields = ["id", "name", "description", "URL", "category", "language", "country"]
# ^^^ This line gives the same result as the line below, but assuming that all objects follow a standard response, the line below lowers the probability of human error :P
fields = finalSources[0].keys()
with open("sources.csv", "w") as file:
    dictWriter = csv.DictWriter(file, fieldnames=fields)
    dictWriter.writeheader()
    dictWriter.writerows(finalSources)

