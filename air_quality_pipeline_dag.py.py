from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
import smtplib
import subprocess

default_args = {
    'owner': 'sunbeam',
    'depends_on_past': False,
    'email': ['shahanashirin202@gmail.com'],
    'email_on_failure': True,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='air_quality_data_pipeline',
    default_args=default_args,
    description='End-to-end air quality pipeline from Kafka to Superset',
    schedule_interval="* * * * *",
    catchup=False
)

# -------------------------------------
# 1. Start Kafka Producer (Python script)
# -------------------------------------
kafka_producer_task = BashOperator(
    task_id='start_kafka_producer',
    bash_command='python3 /home/sunbeam/BIgdata_project/kafka_producer_airquality.py',
    dag=dag
)

# -------------------------------------
# 2. Start Spark Streaming Consumer
# -------------------------------------
spark_stream_task = BashOperator(
    task_id='start_spark_streaming',
    bash_command='spark-submit /home/sunbeam/BIgdata_project/spark_stream_kafka_consumer.py',
    dag=dag
)

# -------------------------------------
# 3. Hive Aggregation Task
# -------------------------------------
hive_aggregate_query = """
CREATE TABLE IF NOT EXISTS air_quality_summary STORED AS PARQUET AS
SELECT
  city,
  date_format(ts, 'yyyy-MM-dd HH:00:00') AS hour_window,
  ROUND(AVG(aqi), 2) AS avg_aqi,
  MAX(aqi) AS max_aqi
FROM air_quality_data
WHERE ts >= current_timestamp() - interval 1 hour
GROUP BY city, date_format(ts, 'yyyy-MM-dd HH:00:00');
"""

hive_aggregation_task = HiveOperator(
    task_id='aggregate_hourly_data',
    hql=hive_aggregate_query,
    hive_cli_conn_id='hive_conn_id',
    dag=dag
)

# -------------------------------------
# 4. Alert if AQI > 200 (Python email)
# -------------------------------------
def send_alert_email():
    query = """
        SELECT city, aqi FROM air_quality_data
        WHERE aqi > 200
        AND ts >= current_timestamp() - interval 1 hour;
    """
    cmd = f"hive -e \"{query}\""
    result = subprocess.getoutput(cmd)

    if result.strip() and "city" not in result:
        msg = f"""Subject: AQI Alert 🚨

The following cities have AQI > 200 in the past hour:

{result}
        """
        server = smtplib.SMTP('localhost')
        server.sendmail("airflow@localhost", "shahanashirin202@gmail.com", msg)
        server.quit()

alert_task = PythonOperator(
    task_id='send_alert_if_aqi_exceeds',
    python_callable=send_alert_email,
    dag=dag
)

# -------------------------------------
# DAG Workflow
# -------------------------------------
kafka_producer_task >> spark_stream_task >> hive_aggregation_task >> alert_task
