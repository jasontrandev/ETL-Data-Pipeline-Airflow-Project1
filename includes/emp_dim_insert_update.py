import os
from dotenv import load_dotenv
import boto3
import numpy as np
import pandas as pd
from datetime import datetime
import snowflake.connector as sc
from airflow.decorators import task

import queries

# Load the .env file
dotenv_path = os.path.join("/opt/airflow", ".env")
load_dotenv(dotenv_path)

# Get the AWS S3 credentials from .env
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Get the Snowflake credentials from .env
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')


@task(multiple_outputs=True)
def detect_new_or_changed_rows():
    ################################### Connect to S3 #####################################
    s3_client = boto3.client(
        's3',
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY
    )
    obj = s3_client.get_object(Bucket='etl-airflow-bucket-demo', Key='Jason_emp_data.csv')
    emp_detail = pd.read_csv(obj['Body'], index_col=False)
    emp_detail.drop(emp_detail.columns[0], axis=1, inplace=True)
    print("Succesfully read data from AWS S3")

    # Rename employee number data
    src_emp_df = emp_detail.rename(columns={"employee_number": "employee_id"}, inplace=False)

    # Drop employee_status column as it's not part of employee dimension
    src_emp_df.drop('employment_status', axis=1, inplace=True)

    ################################### Connect to DWH ###################################
    # Use your Snowflake user credentials to connect
    conn = sc.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA,
    )
    print("Connected to Snowflake DWH succesfully")

    # Create your SQL command
    sql = queries.SELECT_DWH_EMP_DIM

    # Create the cursor object with your SQL command
    cursor = conn.cursor()
    cursor.execute(sql)
    print("Query executed")

    # Convert output to a dataframe
    tgt_emp_df = cursor.fetch_pandas_all()
    print("Data fetched")
    conn.close()

    # Columns renaming
    src_emp_df.columns = ["src_" + col for col in src_emp_df.columns]
    tgt_emp_df.columns = ["tgt_" + col.lower() for col in tgt_emp_df.columns]
    print("Columns renamed")

    # Join source & target data and add effective dates and active flag
    src_plus_tgt = pd.merge(src_emp_df, tgt_emp_df, how='left', left_on='src_employee_id', right_on='tgt_employee_id')
    src_plus_tgt['effective_start_date'] = datetime.now().date().strftime("%Y-%m-%d")
    src_plus_tgt['effective_end_date'] = None
    src_plus_tgt['is_active'] = "Y"

    # Get new rows only (i.e. rows that doesn't exits in DWH, all DWH data will be null)
    new_inserts = src_plus_tgt[src_plus_tgt.tgt_employee_id.isna()].copy()

    # Replace NaN values with None (which translates to NULL in SQL)
    new_inserts = new_inserts.replace({pd.NA: None, np.nan: None})

    # Select only source columns and the effective dates and active flag
    cols_to_insert = src_emp_df.columns.tolist() + ['effective_start_date', 'effective_end_date', 'is_active']

    # Generate new rows to insert and convert result to string
    result_list = new_inserts[cols_to_insert].values.tolist()
    result_tuple = [tuple(row) for row in result_list]
    new_rows_to_insert = ", ".join(str(row).replace('None', 'NULL') for row in result_tuple)
    print(f"Found {len(result_list)} new rows")

    # Get changed rows only (i.e. rows that exist in DWH but with different supevisor)
    insert_updates = src_plus_tgt[(src_plus_tgt['src_supervisor_name'] != src_plus_tgt['tgt_supervisor_name']) & (~src_plus_tgt.tgt_employee_id.isna())]

    # Replace NaN values with None
    insert_updates = insert_updates.replace({pd.NA: None, np.nan: None})

    # Generate changed rows to insert and convert result to string
    insert_list = insert_updates[cols_to_insert].values.tolist()
    insert_tuple = [tuple(row) for row in insert_list]
    changed_rows_to_insert = ", ".join(str(row).replace('None', 'NULL') for row in insert_tuple)

    # The resulted string will be sent to the next task (SnowflakeOperator) to use in insert query
    if changed_rows_to_insert == '':
        rows_to_insert = new_rows_to_insert
    else:
        rows_to_insert = new_rows_to_insert + ", " + changed_rows_to_insert

    # Get emp_ids to update
    ids_to_update = insert_updates.src_employee_id.tolist()
    print(f"Found {len(ids_to_update)} changed rows")
    
    # This result will be sent to the next task (SnowflakeOperator) to use in update query
    ids_to_update = ", ".join([f"'{id}'"for id in ids_to_update])

    return {"rows_to_insert": rows_to_insert, "ids_to_update": ids_to_update}