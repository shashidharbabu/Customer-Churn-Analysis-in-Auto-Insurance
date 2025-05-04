from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import snowflake.connector
import logging
from airflow.models import Variable  # ✅ keep this for snowflake variables

def copy_into_snowflake(table_name, s3_path, database='USER_DB_FALCON'):
    conn = None
    cs = None
    fq_table = f"{database}.RAW.{table_name}"  # fully-qualified table name

    try:
        logging.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=Variable.get('snowflake_user'),
            password=Variable.get('snowflake_password'),
            account=Variable.get('snowflake_account'),
            warehouse=Variable.get('snowflake_warehouse'),
            database=database,
            schema='RAW'
        )
        cs = conn.cursor()

        cs.execute(f"USE DATABASE {database}")
        cs.execute(f"USE SCHEMA RAW")

        logging.info(f"Starting transaction for {fq_table}")
        cs.execute("BEGIN;")

        logging.info(f"Truncating {fq_table}")
        cs.execute(f"TRUNCATE TABLE {fq_table}")

        sql = f"""
            COPY INTO {fq_table}
            FROM '{s3_path}'
            STORAGE_INTEGRATION = MY_S3_INTEGRATION
            FILE_FORMAT = MY_CSV_FORMAT
            ON_ERROR = 'CONTINUE';
        """
        logging.info(f"Executing COPY INTO for {fq_table}")
        cs.execute(sql)

        logging.info(f"Committing transaction for {fq_table}")
        cs.execute("COMMIT;")
        logging.info(f"Load completed for {fq_table}")

    except Exception as e:
        logging.error(f"Error loading {fq_table}: {e}")
        if cs:
            cs.execute("ROLLBACK;")
            logging.info(f"Transaction rolled back for {fq_table}")
        raise
    finally:
        if cs:
            cs.close()
        if conn:
            conn.close()
        logging.info(f"Connection closed for {fq_table}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 3),
    'retries': 1
}

with DAG(
    dag_id='s3_to_snowflake_churn_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL DAG to load auto insurance churn data from S3 into Snowflake RAW schema'
) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('load_data_to_snowflake') as load_data_to_snowflake:
        for table_name in ['address', 'customer', 'demographic', 'termination', 'autoinsurance_churn']:
            PythonOperator(
                task_id=f'copy_{table_name}_data_to_snowflake',
                python_callable=copy_into_snowflake,
                op_kwargs={
                    'table_name': table_name,
                    's3_path': f's3://data226project/{table_name}.csv'
                }
            )

    end = DummyOperator(task_id='end')

    start >> load_data_to_snowflake >> end
