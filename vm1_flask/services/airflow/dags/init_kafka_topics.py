from pendulum import datetime, from_format, duration
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_provider_kafka.hooks.admin_client import AdminClient, NewTopic
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator
import json


def fn_topics_create():
    admin_client = AdminClient({
        "bootstrap.servers": "broker1:29092"
    })

    topic_list = []
    topic_list.append(NewTopic("test_work", 3, 1))
    topic_list.append(NewTopic("data", 3, 1))
    # topic_list.append(NewTopic("subdata", 3, 1))
    # topic_list.append(NewTopic("metadata", 3, 1))
    topic_list.append(NewTopic("features", 3, 1))
    topic_list.append(NewTopic("predicts", 3, 1))
    futures = admin_client.create_topics(topic_list)
    for t, f in futures.items():
        f.result()

connection_config = {
    "bootstrap.servers": 'broker1:29092'
}


def fn_produce_to_topic_test():
    for i in range(5):
        yield (json.dumps(i), json.dumps(i + 1))


default_args = {'start_date': datetime(2022, 12, 2, 15, tz="Europe/Moscow"),
                'retries': 5,
                'retry_delay': duration(seconds=15),
                }

with DAG(
        dag_id='000_init_kafka',
        default_args=default_args,
        catchup=False,
        schedule_interval='* * * * *',
) as dag:
    # topics_create = PythonOperator(
    #     task_id='topics_create',
    #     python_callable=fn_topics_create,
    #     )
    producer_test_work = ProduceToTopicOperator(
        task_id=f"produce_to_test_work",
        topic='test_work',
        producer_function=fn_produce_to_topic_test,
        kafka_config=connection_config
    )

    producer_test_work