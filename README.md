
# Building a Data System with Airflow

The goal of this practice is to build a system that logs the daily price of different stocks.

## Instructions

1. Setup Airflow using the official `docker-compose` YAML file. This can be obtained here:
    https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose-yaml
    
    Before starting we suggest changing one line in the docker-compose file to disable the automatic loading of examples to avoid UI clutter:
    ```
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
    ```
    to:
    ```
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    ```
    
    By default this setup creates a `dags` directory in the host from which Airflow obtains the DAG definitions.

    After the initial setup an Airflow instance should be reachable in `http://localhost:8080/home` the default username and password are `airflow`:`airflow`.

2. Create another database in the Postgres used by Airflow to store the stocks data.

3. Develop the data model to store the daily stock data (symbol, date, open, high, low, close) using SQLAlchemy's declarative base. Then create the tables in the DB.

4. Create a Python class, similar to the `SqLiteClient` of the practical Airflow coursework, in order to connect with the Postgres DB. You should implement the same methods present in the `SqLiteClient`. Bonus: Try to write a parent/base DB API class and make the `SqLite` and Postgres client inherit from it.

5. Develop a DAG that obtains the price information of Google (GOOG), Microsoft (MSFT) and Amazon (AMZN) and then inserts the data in the database using the Python class developed in the previous point.
   For this we suggest using the following API:
   https://www.alphavantage.co/

   The Python files that define the DAGs should be placed in the `dags` directory previosly mentioned. 

6. Add another task the that depends on the first one that fetches data from the database of the last week and produces a plot of the value of each stock during said time period.

7. Add two unit tests runnable with [pytest](https://docs.pytest.org/) that can be run from the commandline:
    - One that tests the extraction. This refers to the formatting that takes place after the data is fetched from the API that is to be inserted in the DB. 
    - Another for the aggregation of the values used for plotting after they are extracted from the DB.

8. Implement a CI step using [GitHub Actions](https://docs.github.com/en/actions) to run the unit tests using `pytest` each time a commit is pushed to a branch in a PR. In case of failure the result should be visible in GitHub's merge request UI.


## Extras
Using the suggested `docker-compose` setup you can access the database using `airflow` as both busername and password in the following way:
```
$ sudo docker-compose exec airflow-webserver psql -h postgres
Password for user airflow: 
psql (11.13 (Debian 11.13-0+deb10u1), server 13.4 (Debian 13.4-4.pgdg110+1))
WARNING: psql major version 11, server major version 13.
         Some psql features might not work.
Type "help" for help.

airflow=# 
```

In the same way you can open a shell to work inside the Docker containers using:
```
sudo docker-compose exec airflow-webserver /bin/bash
```
This can be useful when creating the tables that will hold the data.

When connecting to the DB from inside the container you can use the default value of the `AIRFLOW__CORE__SQL_ALCHEMY_CONN` variable defined in the compose file.

## Bonus points

If you want to go an extra mile you can do the following:
* Add the configs for [Pylint](https://pylint.org/) and [black](https://black.readthedocs.io/en/stable/).
* Implement a CI step using [GitHub Actions](https://docs.github.com/en/actions) to run Pylint each time a commit is pushed to a branch in a PR.

# Resolution

This section describes the considerations was take into account in order to resolve the requirements presented.

📎 First, was used the same postgres service defined in the airflow yaml (to minimize the use of ram resources in local VM), also is possible (and **mandatory** in prod environments) to define another postgres service in the yaml and point it using the variables defined in the `/dags/utils/config.py` file:

* stocks_conn_user = 'airflow'
* stocks_conn_pass = 'airflow'
* stocks_conn_host = 'postgres'
* stocks_conn_port = '5432'
* stocks_conn_db = 'airflow'

📎 Related to the data model, in the same config.py file are another two variables used to parametrize the schema and table name. 

* stocks_conn_schema = 'itba_stock_ticker'
* stocks_conn_daily_ticker_table = 'stock_ticker_daily'

This two variables are used in the SQL create queries presented in the `stocks_etl_dag.py` and are executed in the first dag task `create_schema_table_if_not_exists` with IF NOT EXISTS.  

📎 The `PostgresqlClient` (dags/utils/postgresql_cli.py) python class was developed in order to connect with the Postgres DB following the `SqLiteClient` class of the practical Airflow coursework. The main difference of that class is in the db_uri (that use all the postgres necessary params) and the possibility to consider the schema name in the `insert_from_frame` method.


👷‍♂️ ⚙️ Now, in reference of the `stocks_etl_dag` developed, consists in three main steps:
![Image of the Deployment](https://github.com/flanfranco/itba-cde-tpf-python-applications/blob/main/documentation/resources/images/01_stocks_etl_dag.png)

📌 Obtaining the daily data from the stock api, filtering (by execution day) and using xcom passing it to the another task.

ℹ️ One important thing to take into account in this task, is that the response obtained from stock api is stored in a 📁 raw folder (before to process it). In prod environments is convenient (a good practice) storing (for example in a S3 bucket) the raw data, and then, in a subsequent stg/process task access it, process it and then continues with the pipeline. This daily get task also has the following variables to configure (/dags/utils/config.py) the stock api request:
* stocks_symbols_list = ["GOOG", "MSFT", "AMZN"]
* stocks_api_base_url = 'https://www.alphavantage.co/query'
* stocks_api_function = 'TIME_SERIES_DAILY'
* stocks_api_key = 'pippo e pluto'

![Image of the Deployment](https://github.com/flanfranco/itba-cde-tpf-python-applications/blob/main/documentation/resources/images/04_raw_data_folder.png)

📌 Storing the daily stock data to 💾 postgresql database (pulling it from xcom) using PostgresqlClient. 

ℹ️ In this case the xcom pulling methodology was used to understand how it works, in a prod environment is convenient to read the data from a S3 bucket for example in accordance that was commented in the previous point.

📌 Executing a SQL query that generates the weekly report aggregating the daily ticker data from postgresql database and then generate the  📈 weekly report with numpy.

![Image of the Deployment](https://github.com/flanfranco/itba-cde-tpf-python-applications/blob/main/documentation/resources/images/05_reports_folder.png)
![Image of the Deployment](https://github.com/flanfranco/itba-cde-tpf-python-applications/blob/main/documentation/resources/images/06_example_weekly_report.png)

ToDo: comment best practices take in code:
https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
