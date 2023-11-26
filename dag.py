import os
os.system("pip install s3fs")
os.system("pip install psycopg2-binary")
from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from s3fs.core import S3FileSystem
import requests
import pandas as pd
import s3fs
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
from ETL_scripts.transform import transform_data
# Function to fetch data from the API and save it to S3
def fetch_data(**kwargs):
    # API endpoint
    api_endpoint = "https://data.austintexas.gov/resource/9t4d-g238.json"

    # Fetch data from the API
    response = requests.get(api_endpoint)
    data = response.json()

    # Convert data to DataFrame
    df = pd.DataFrame(data)
    print("Download Successful")
    
    # Return the DataFrame to pass it to the next task
    return df

def save_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='Extract')

    # Upload the CSV file to S3
    s3_bucket = "yamini-airflow-storage"
    s3_key = "austin_animal_shelter_data.csv"

    # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"

    # Save to S3
    df.to_csv(f"s3://{s3_bucket}/{s3_key}", index=False, storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key
    })
def execute_sql_script(engine, sql_script):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Execute the SQL script
            connection.execute(sql_script)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            raise e
# def load_to_redshift(**kwargs):
#     # AWS credentials
#     aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
#     aws_secret_access_key = "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"

#     # Redshift credentials and connection information
#     redshift_user = "admin"
#     redshift_password = "Yamini27"
#     redshift_host = "airflow.662892316736.us-east-1.redshift-serverless.amazonaws.com"
#     redshift_port = 5439
#     redshift_db = "dev"

#     # S3 bucket and key
#     s3_bucket = "yamini-airflow-storage"
#     #s3_key = "your_data_folder"

#     # Initialize S3 filesystem
#     s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
#     # Inside load_data_to_redshift task
#     # kwargs['ti'].xcom_push(key='redshift_params', value=redshift_params)

#     # # Retrieve Redshift connection parameters from XCom
#     # redshift_params = kwargs['ti'].xcom_pull(task_ids='airflow', key='redshift_params')

#     # # Create Redshift engine
#     # redshift_conn_str = f"postgresql+psycopg2://{redshift_params['redshift_user']}:{redshift_params['redshift_password']}@{redshift_params['redshift_host']}:{redshift_params['redshift_port']}/{redshift_params['redshift_db']}"
#     # engine = create_engine(redshift_conn_str)

#     # Redshift connection string
#     redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

#     # Create Redshift engine
#     engine = create_engine(redshift_conn_str, poolclass= NullPool)
#     engine.execute("SET json_serialization_enable TO false")
#     # Read and execute the init.sql file
#     with open("dags/db/init.sql", "r") as init_sql_file:
#         init_sql_script = init_sql_file.read()
#         execute_sql_script(engine, init_sql_script)
# # 'dim_animals', 
# #     List of tables to load
#     table_names = ['dim_dates', 'dim_outcome_types']

#     for table_name in table_names:
#         # Define S3 file path
#         s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"

#         # Load data into Redshift using COPY command
#         # copy_command =  f"COPY {table_name} FROM '{s3_file_path}' CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231120T194044' FORMAT AS CSV IGNOREHEADER 1;"
#         copy_command = f"""
#         COPY {table_name}
#         FROM '{s3_file_path}'
#         CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231121T093456'
#         FORMAT AS csv
#         DELIMITER ',' QUOTE '"'
#         IGNOREHEADER 1
#         REGION AS 'us-east-1';
#         """
#         engine.execute(copy_command)
#         engine.execute("SET json_serialization_enable TO false")
#         print("Data loaded into Redshift successfully")
#     return engine
#     # Store a message in XCom instead of the engine
#     kwargs['ti'].xcom_push(key='result', value='Data loaded into Redshift successfully')


def load_dim_animal(**kwargs):
     # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"

    # Redshift credentials and connection information
    redshift_user = "admin"
    redshift_password = "Yamini27"
    redshift_host = "airflow.662892316736.us-east-1.redshift-serverless.amazonaws.com"
    redshift_port = 5439
    redshift_db = "dev"

    # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"

    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

    # Redshift connection string
    redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

    # Create Redshift engine
    engine = create_engine(redshift_conn_str)

    # Read and execute the init.sql file
    with open("dags/db/init.sql", "r") as init_sql_file:
        init_sql_script = init_sql_file.read()
        execute_sql_script(engine, init_sql_script)
    # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "kVe+32ZTwvqizu8tdmU/Il1eZskSdFwMFqnypp4i"
    table_name = 'dim_animals'
     # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"
    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"
    # Load data into Redshift using COPY command
    copy_command = f"""
    COPY {table_name}
    FROM '{s3_file_path}'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231121T093456'
    FORMAT AS CSV
    DELIMITER ',' QUOTE '"'
    IGNOREHEADER 1
    REGION AS 'us-east-1';
"""
    engine.execute(copy_command)

def load_dim_dates(**kwargs):
     # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"

    # Redshift credentials and connection information
    redshift_user = "admin"
    redshift_password = "Yamini27"
    redshift_host = "airflow.662892316736.us-east-1.redshift-serverless.amazonaws.com"
    redshift_port = 5439
    redshift_db = "dev"

    # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"

    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

    # Redshift connection string
    redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

    # Create Redshift engine
    engine = create_engine(redshift_conn_str)

    # Read and execute the init.sql file
    with open("dags/db/init.sql", "r") as init_sql_file:
        init_sql_script = init_sql_file.read()
        execute_sql_script(engine, init_sql_script)
    #AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "kVe+32ZTwvqizu8tdmU/Il1eZskSdFwMFqnypp4i"
    table_name = 'dim_dates'
     # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"
    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"
    # Load data into Redshift using COPY command
    print("loading")
    copy_command = f"""
    COPY {table_name}
    FROM '{s3_file_path}'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231121T093456'
    FORMAT AS CSV
    DELIMITER ',' QUOTE '"'
    IGNOREHEADER 1
    REGION AS 'us-east-1';
"""

    engine.execute(copy_command)
    print("Loading successful")

def load_dim_outcome_types(**kwargs):
     # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"

    # Redshift credentials and connection information
    redshift_user = "admin"
    redshift_password = "Yamini27"
    redshift_host = "airflow.662892316736.us-east-1.redshift-serverless.amazonaws.com"
    redshift_port = 5439
    redshift_db = "dev"

    # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"

    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

    # Redshift connection string
    redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

    # Create Redshift engine
    engine = create_engine(redshift_conn_str)

    # Read and execute the init.sql file
    with open("dags/db/init.sql", "r") as init_sql_file:
        init_sql_script = init_sql_file.read()
        execute_sql_script(engine, init_sql_script)
    # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "kVe+32ZTwvqizu8tdmU/Il1eZskSdFwMFqnypp4i"
    table_name = 'dim_outcome_types'
     # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"
    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"
    # Load data into Redshift using COPY command
    copy_command =  f"COPY {table_name} FROM '{s3_file_path}' CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231120T194044' FORMAT AS CSV IGNOREHEADER 1;"
    engine.execute(copy_command)


def load_fct_outcomes(**kwargs):
     # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"

    # Redshift credentials and connection information
    redshift_user = "admin"
    redshift_password = "Yamini27"
    redshift_host = "airflow.662892316736.us-east-1.redshift-serverless.amazonaws.com"
    redshift_port = 5439
    redshift_db = "dev"

    # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"

    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)

    # Redshift connection string
    redshift_conn_str = f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_db}"

    # Create Redshift engine
    engine = create_engine(redshift_conn_str)

    # Read and execute the init.sql file
    with open("dags/db/init.sql", "r") as init_sql_file:
        init_sql_script = init_sql_file.read()
        execute_sql_script(engine, init_sql_script)
    # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "kVe+32ZTwvqizu8tdmU/Il1eZskSdFwMFqnypp4i"
    table_name = 'fact_outcomes'
     # S3 bucket and key
    s3_bucket = "yamini-airflow-storage"
    #s3_key = "your_data_folder"
    # Initialize S3 filesystem
    s3 = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    s3_file_path = f"s3://{s3_bucket}/{table_name}.csv"
    # Load data into Redshift using COPY command
    copy_command =  f"COPY {table_name} FROM '{s3_file_path}' CREDENTIALS 'aws_iam_role=arn:aws:iam::662892316736:role/service-role/AmazonRedshift-CommandsAccessRole-20231120T194044' FORMAT AS CSV IGNOREHEADER 1;"
    engine.execute(copy_command)


# DAG definition
with DAG(
    dag_id="dag",
    start_date=datetime(2023, 11, 20),
    schedule_interval='@daily',
) as dag:

    Extract = PythonOperator(
        task_id="Extract",
        python_callable=fetch_data,
    )

    Upload = PythonOperator(
        task_id="Upload",
        python_callable=save_data,
        provide_context=True,  
    )
    Transform_Load = PythonOperator(
        task_id="Transform_Load",
        python_callable=transform_data,
        provide_context=True,  
    )
    
    # load_data_To_redshift = PythonOperator(
    #     task_id="load_data_To_redshift",
    #     python_callable=load_to_redshift,
        
    # )
    load_dim_animaltable = PythonOperator(
        task_id="load_dim_animaltable",
        python_callable=load_dim_animal,
        
    )

    load_dim_datestable = PythonOperator(
        task_id="load_dim_datestable",
        python_callable=load_dim_dates,
        
    )

    load_dim_outcome_typestable = PythonOperator(
        task_id="load_dim_outcome_typestable",
        python_callable=load_dim_outcome_types,
        
    )

    load_fct_outcomestable = PythonOperator(
        task_id="load_fct_outcomestable",
        python_callable=load_fct_outcomes,
        
    )

    
    Extract >> Upload >> Transform_Load >> [load_dim_animaltable,load_dim_datestable,load_dim_outcome_typestable] >> load_fct_outcomestable

    #Extract >> Upload >> Transform_Load >> load_data_To_redshift