# ETL-Data-Pipeline-using-Airflow

## Project Overview

Build an ETL Data Pipelines that uses Airflow DAGs to extract employees’ data from PostgreSQL Schemas, load it into AWS Data Lake, then transform it with Python script, and finally load it into Snowflake Data warehouse using SCD type 2.

## Project Description

![](/static/etl-data-pipeline.png)

The HR department wants to create a headcount dashboard that can show changes over time. Currently, they use multiple reports in Excel to reference these changes.

The idea of the project is to implement and build a data warehouse to store the employees’ information and keep all headcount change history.

The Airflow DAG runs daily to check and extract all new data from the PostgreSQL source, then load it into AWS S3 buckets used as Data Lake containing all raw data as CSV files. After that, some Python functions will be applied to read and transform data. This stage is to extract the new records that will be inserted and the records that will be updated to perform the Slowly Changing Dimension ‘SCD’ concept. The final goal is to keep all historical employees’ changes in the Snowflake Data warehouse.

## Project Structure

    ├── dags              # Contains the Airflow Dag
    ├── logs              # Contains log for each tasks
    ├── plugins           #
    ├── includes	      # Contains the SQL and Python scripts that uses in the AirFlow Dag.
        ├── emp_dim_insert_update.py
        ├── queries.py
    ├── requirements.txt  # Additional Dependencies
    ├── LICENSE
    └── README.md

## Project Steps

1. Deploy Airflow on Docker Compose to install all dependencies and start running Airflow through terminal command.

2. Implement an Airflow DAG that runs daily and uses the TaskFlow approach to pass the outputs from each task to another.
3. Create one task that uses the SqlToS3Operator operation to extract HR data from PostgreSQL schema to AWS S3 buckets in CSV file format.

4. Process data using Python scripts to retrieve the IDs of the new records to insert them in the Data warehouse, and the IDs of the records that contain a few information changes to update it and insert new records with new values for applying the SCD type 2 concept.

5. Load the data into the Snowflake Data warehouse table.

6. The Airflow DAG contains some Python scripts using BranchPythonOperator operation to check if there are new records to insert or records to update before running the task to avoid errors.

## Project Output

In Airflow UI
![](/static/airflow-result.png)

In Snowflake Schema
![](/static/snowflake-result.png)
