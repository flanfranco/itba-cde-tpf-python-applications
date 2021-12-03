
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

