# Project: Data Pipelines
###  Data Pipelines with Airflow / Data Engineer Nanodegree
### Udacity
### Author: Jakub Pitera
___

This projects demonstrates building a data pipeline with Apache Airflow . It builds a dag that orchestrates ETL pipeline from S3 to Redshift database.  

### Pipeline steps: 
1. Create empty tables on redshift
2. Copy staging tables from S3 to Redshift
3. Transform and insert data into fact and dimension tables 
4. Run quality checks

### Files description:
* dags/udac_example_dag.py - main script, defines dag, creates task with appropriate operators and parameters and schedules pipeline
* create_tables.sql - sql queries for dropping and creating tables, includes data definition
* plugins/helpers/sql_queries.py - additional queries used by airflow operators, used for inserting into tables
* plugins/operators/stage_redshift.py - definition of custom airflow operator for staging from S3 to redshift
* plugins/operators/load_fact.py - definition of custom airflow operator inserting data from staging table into fact table on redshift
* plugins/operators/load_dimension.py - definition of custom airflow operator inserting data from staging table into dim tables on redshift
* plugins/operators/data_quality.py - definition of custom airflow operator for quality check on data
* README.md - documentation