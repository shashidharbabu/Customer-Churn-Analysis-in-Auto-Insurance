from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import snowflake.connector
import logging

def copy_into_snowflake(table_name, s3_path, file_format="(type = 'CSV', skip_header = 1, field_optionally_enclosed_by='\"')"):
    """
    Copies data from S3 to Snowflake raw schema table inside explicit transaction.
    Ensures idempotency by truncating table before load.
    Rolls back transaction if any error occurs.
    """
    conn = None
    cs = None
    try:
        logging.info(f"Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user='{{ var.value.snowflake_user }}',
            password='{{ var.value.snowflake_password }}',
            account='{{ var.value.snowflake_account }}',
            warehouse='{{ var.value.snowflake_warehouse }}',
            database='{{ var.value.snowflake_database }}',
            schema='RAW'
        )
        cs = conn.cursor()

        logging.info(f"Starting transaction for RAW.{table_name}")
        cs.execute("BEGIN;")

        logging.info(f"Truncating table RAW.{table_name}")
        cs.execute(f"TRUNCATE TABLE RAW.{table_name};")

        sql = f"""
            COPY INTO RAW.{table_name}
            FROM '{s3_path}'
            STORAGE_INTEGRATION = my_s3_integration
            FILE_FORMAT = {file_format};
        """
        logging.info(f"Executing COPY INTO for RAW.{table_name}")
        cs.execute(sql)

        logging.info(f"Committing transaction for RAW.{table_name}")
        cs.execute("COMMIT;")
        logging.info(f"Load completed for RAW.{table_name}")

    except Exception as e:
        logging.error(f"Error loading RAW.{table_name}: {e}")
        if cs:
            cs.execute("ROLLBACK;")
            logging.info(f"Transaction rolled back for RAW.{table_name}")
        raise
    finally:
        if cs:
            cs.close()
        if conn:
            conn.close()
        logging.info(f"Connection closed for RAW.{table_name}")

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
    description='ETL DAG to load auto insurance churn data from S3 into Snowflake RAW schema with transactional loading'
) as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup('load_data_to_snowflake') as load_data_to_snowflake:

        copy_address = PythonOperator(
            task_id='copy_address_data_to_snowflake',
            python_callable=copy_into_snowflake,
            op_kwargs={
                'table_name': 'address',
                's3_path': 's3://data226project/address.csv'
            }
        )

        copy_customer = PythonOperator(
            task_id='copy_customer_data_to_snowflake',
            python_callable=copy_into_snowflake,
            op_kwargs={
                'table_name': 'customer',
                's3_path': 's3://data226project/customer.csv'
            }
        )

        copy_demographic = PythonOperator(
            task_id='copy_demographic_data_to_snowflake',
            python_callable=copy_into_snowflake,
            op_kwargs={
                'table_name': 'demographic',
                's3_path': 's3://data226project/demographic.csv'
            }
        )

        copy_termination = PythonOperator(
            task_id='copy_termination_data_to_snowflake',
            python_callable=copy_into_snowflake,
            op_kwargs={
                'table_name': 'termination',
                's3_path': 's3://data226project/termination.csv'
            }
        )

        copy_autoinsurance_churn = PythonOperator(
            task_id='copy_autoinsurance_churn_data_to_snowflake',
            python_callable=copy_into_snowflake,
            op_kwargs={
                'table_name': 'autoinsurance_churn',
                's3_path': 's3://data226project/autoinsurance_churn.csv'
            }
        )

    end = DummyOperator(task_id='end')

    # Define flow
    start >> load_data_to_snowflake >> end
