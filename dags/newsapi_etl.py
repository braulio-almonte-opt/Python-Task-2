from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
import pendulum
import dotenv
import os

dotenv.load_dotenv()
projectID = os.getenv("GOOGLE_CONSOLE_PROJECT_ID")

with DAG(
    'newsapi_etl',
    start_date=pendulum.datetime(2026, 2, 4, tz="EST"),
    schedule="0 8 * * *",
    catchup=False,
) as dag:

    run_etl_pod = KubernetesPodOperator(
        task_id="run_etl_in_k8s",
        image="pythontask2-news-etl:latest",
        namespace="default",
        name="etl-pod",
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        in_cluster=False, 
        config_file="~/.kube/config"
    )

    check_articles = BigQueryCheckOperator(
        task_id="check_articles_data",
        sql=f"SELECT COUNT(*) FROM `{projectID}.news.articles` WHERE DATE(publish_time) = CURRENT_DATE()",
        use_legacy_sql=False,
        project_id=projectID
    )

    check_sources = BigQueryCheckOperator(
            task_id="check_sources_data",
            sql=f"SELECT COUNT(*) FROM `{projectID}.news.sources`",
            use_legacy_sql=False,
            project_id=projectID
        )

run_etl_pod >> [check_articles, check_sources]