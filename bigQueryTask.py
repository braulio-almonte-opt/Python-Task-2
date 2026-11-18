from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()
projectID = os.getenv("GOOGLE_CONSOLE_PROJECT_ID")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

datasetID = f"{projectID}.articlesDataset"

client = bigquery.Client(project=projectID)
dataset = bigquery.Dataset(datasetID)
dataset.location = "US"
dataset = client.create_dataset(dataset=dataset, exists_ok=True)

articlesTableID = f"{datasetID}.articles"
sourcesTableID = f"{datasetID}.sources"

jobConfig = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect = True,
    skip_leading_rows=1
)

with open("articles.csv", "rb") as file:
    articlesJob = client.load_table_from_file(
        file,
        articlesTableID,
        job_config=jobConfig
    )
articlesJob.result()

sourceSchema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("name", "STRING"),  
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("language", "STRING"),
    bigquery.SchemaField("country", "STRING")
]

sourcesJobConfig = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect = True,
    schema=sourceSchema,
    skip_leading_rows=1
)

with open("sources.csv", "rb") as file:
    sourcesJob = client.load_table_from_file(
        file,
        sourcesTableID,
        job_config=sourcesJobConfig
    )

sourcesJob.result()