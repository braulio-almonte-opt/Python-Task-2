from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pendulum
import dotenv
import os

dotenv.load_dotenv()
projectID = os.getenv("GOOGLE_CONSOLE_PROJECT_ID")
if not projectID:
    raise RuntimeError("Set GOOGLE_CONSOLE_PROJECT_ID in the Airflow environment")

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
        image_pull_policy="IfNotPresent",
        is_delete_operator_pod=True,
        in_cluster=False, 
        config_file="~/.kube/config",
        env_vars={
            "GOOGLE_CONSOLE_PROJECT_ID": projectID,
            "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/credentials.json",
        },
        secrets=[
            Secret(
                deploy_type="env",
                deploy_target="NEWS_API_KEY",
                secret="newsapi-secrets",
                key="NEWS_API_KEY",
            ),
            Secret(
                deploy_type="volume",
                deploy_target="/var/secrets/google",
                secret="gcp-credentials",
            ),
        ],
    )

    check_articles = BigQueryCheckOperator(
        task_id="check_articles_data",
        sql=f"SELECT COUNT(*) FROM `{projectID}.news.articles` WHERE DATE(publish_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE();",
        use_legacy_sql=False,
        project_id=projectID
    )

    check_sources = BigQueryCheckOperator(
            task_id="check_sources_data",
            sql=f"SELECT COUNT(*) FROM `{projectID}.news.sources`;",
            use_legacy_sql=False,
            project_id=projectID
        )

    copy_sources_to_v2 = BigQueryInsertJobOperator(
        task_id="copy_sources_to_v2",
        configuration={
            "query": {
                "query": (
                    f"CREATE OR REPLACE TABLE `{projectID}.news_v2.sources` AS "
                    f"SELECT * FROM `{projectID}.news.sources`;"
                ),
                "useLegacySql": False,
            }
        },
        project_id=projectID,
        location="US",
        gcp_conn_id="google_cloud_default",
        deferrable=False,
    )

    copy_articles_to_v2 = BigQueryInsertJobOperator(
        task_id="copy_articles_to_v2",
        configuration={
            "query": {
                "query": (
                    f"CREATE OR REPLACE TABLE `{projectID}.news_v2.articles` AS "
                    f"SELECT * FROM `{projectID}.news.articles`;"
                ),
                "useLegacySql": False,
            }
        },
        project_id=projectID,
        location="US",
        gcp_conn_id="google_cloud_default",
        deferrable=False,
    )

run_etl_pod >> [check_articles, check_sources] >> copy_sources_to_v2 >> copy_articles_to_v2