from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator

dag_args = {
    'owner': 'shahana',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 8, 8),
    'email_on_failure': True,
    'email': ['shahanashirin202@gmail.com']
}

with DAG(
    dag_id='pipeline_airquality',
    default_args=dag_args,
    schedule_interval='@daily',
    catchup=False,
    description='Daily air quality summary overwrite'
) as dag:

    overwrite_summary = HiveOperator(
        task_id='overwrite_air_quality_summary',
        hive_cli_conn_id='my_hive_conn',
        hql="""
            USE air_quality;

            INSERT OVERWRITE TABLE air_quality_summary
            PARTITION (dt = '{{ ds }}')
            SELECT
                city,
                ROUND(AVG(aqi), 2) AS avg_aqi,
                MAX(aqi) AS max_aqi
            FROM air_quality_data
            WHERE dt = '{{ ds }}'
            GROUP BY city;
        """
    )
    
    

    send_email = EmailOperator(
        task_id='send_email',
        to='shahaashiriisha@gmail.com',
        subject='Air Quality Summary - {{ ds }}',
        html_content="""
            <h3>Air Quality Aggregation Completed</h3>
            <p>Your Hive aggregation task has finished successfully for date {{ ds }}.</p>
        """,
    )

    # Example chaining — run email after Hive aggregation
    overwrite_summary >> send_email

