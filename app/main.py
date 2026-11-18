from newsapi import NewsApiClient
from dotenv import load_dotenv
from google.cloud import bigquery
import os
import datetime
import pandas as pd

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
projectID = os.getenv("GOOGLE_CONSOLE_PROJECT_ID")
API_KEY = os.getenv("NEWS_API_KEY")
newsapi = NewsApiClient(api_key=API_KEY)

today = datetime.datetime.today()
dateDelay = today + datetime.timedelta(days=-3)
todayString = today.strftime("%Y-%m-%d")
dateDelayString = dateDelay.strftime("%Y-%m-%d")

scienceSources = newsapi.get_sources(country="us", category="science")
businessSources = newsapi.get_sources(country="us", category="business")

requestedSources = scienceSources | businessSources
finalSources = requestedSources["sources"]

topArticles = newsapi.get_everything(sources=", ".join([source["id"] for source in finalSources]), 
                                        sort_by="popularity", language="en", 
                                        page_size=100, 
                                        from_param=dateDelayString, 
                                        to=todayString)["articles"]

sources_df = pd.DataFrame(finalSources)
articles_df = pd.DataFrame([
    {
        "source": article["source"]["name"],
        "author": article.get("author"),
        "title": article["title"],
        "url": article["url"],
        "publish_time": article["publishedAt"]
    }
    for article in topArticles
])

articles_df["publish_time"] = pd.to_datetime(
    articles_df["publish_time"],
    utc=True,
    errors="coerce"
)

datasetID = f"{projectID}.news"

client = bigquery.Client(project=projectID)
dataset = bigquery.Dataset(datasetID)
dataset.location = "US"
dataset = client.create_dataset(dataset=dataset, exists_ok=True)

versionedDatasetID = f"{projectID}.news_v2"
versionedDataset = bigquery.Dataset(versionedDatasetID)
versionedDataset.location = "US"
client.create_dataset(dataset=versionedDataset, exists_ok=True)

sourcesTableID = f"{datasetID}.sources"
articlesTableID = f"{datasetID}.articles"

sourcesSchema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("name", "STRING"),  
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("country", "STRING")
]

articlesSchema = [
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("author", "STRING"),  
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("publish_time", "TIMESTAMP")
]


sourcesTableJobConfig = bigquery.LoadJobConfig(
    schema=sourcesSchema,
    write_disposition="WRITE_TRUNCATE"
)

articlesTableJobConfig = bigquery.LoadJobConfig(
    schema=articlesSchema,
    write_disposition="WRITE_TRUNCATE"
)

client.create_dataset(dataset=dataset, exists_ok=True)

sourcesLoadJob = client.load_table_from_dataframe(
    dataframe=sources_df,
    destination=sourcesTableID,
    job_config=sourcesTableJobConfig
)

sourcesLoadJob.result()

articlesLoadJob = client.load_table_from_dataframe(
    dataframe=articles_df,
    destination=articlesTableID,
    job_config=articlesTableJobConfig
)
articlesLoadJob.result()
