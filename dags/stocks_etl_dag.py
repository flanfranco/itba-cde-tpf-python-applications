
# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# DB handle imports
from utils.postgresql_cli import PostgresqlClient 
import sqlalchemy.exc

# General use imports
import utils.config as conf


# SQL queries section -------------------------------------------------------------------------
SQL_CREATE_STOCK_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS  {conf.stocks_conn_schema}
"""
SQL_CREATE_DAILY_TICKER_TABLE = f"""
CREATE TABLE IF NOT EXISTS  {conf.stocks_conn_schema}.{conf.stocks_conn_daily_ticker_table} (
    symbol text NOT NULL,
	date date NOT NULL,
	open float NULL,
	high float NULL,
	low float NULL,
	close float NULL,
	volume int NULL,
    date_time_dag_run TIMESTAMP
)
"""
SQL_WEEKLY_REPORT = f"""
select distinct 
	symbol, 
	date, 
	((high + low) / 2) as avg_price
from {conf.stocks_conn_schema}.{conf.stocks_conn_daily_ticker_table} 
where 
	(	
	date >= date_trunc('week', TO_DATE('{{execution_date}}','YYYY-MM-DD')) and
    date < (date_trunc('week', TO_DATE('{{execution_date}}','YYYY-MM-DD')) + interval '5 days')
    )
order by 
	date,
	symbol 
"""
# --------------------------------------------------------------------------------------------


# Python callables section -------------------------------------------------------------------
def create_schema_table_if_not_exists():
    sql_cli = PostgresqlClient(conf.stocks_conn_user,conf.stocks_conn_pass, conf.stocks_conn_host, conf.stocks_conn_port, conf.stocks_conn_db, conf.stocks_conn_schema)
    print(f"Sql client: {sql_cli}...")
    try:
        sql_cli.execute(SQL_CREATE_STOCK_SCHEMA)
        sql_cli.execute(SQL_CREATE_DAILY_TICKER_TABLE)
    except sqlalchemy.exc.IntegrityError:
        print("Integrity Error")

def get_daily_stock_data_json(stock_ticker_symbol, **context):   
    import os 
    import json
    import requests
    from time import sleep
    import numpy as np
    import pandas as pd

    execution_date = f"{context['execution_date']:%Y-%m-%d}"
    print(f"Execution date: {execution_date}...")

    end_point = f'{conf.stocks_api_base_url}?function={conf.stocks_api_function}&symbol={stock_ticker_symbol}&apikey={conf.stocks_api_key}&datatype=json'
    print(f"Getting data from {end_point}...")
    try:
        response = requests.get(end_point)
        sleep(61)  # To handle api limits
    except requests.exceptions.ConnectionError:
        print("Connection Error")
    except requests.exceptions.RequestException:
        print("Request Exception") 
    
    # Store the (raw) request response 
    file_name = f"{context['execution_date']:%Y%m%d}" + f'_{stock_ticker_symbol}.json'
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'daily_stock_data',file_name)
    print(f"Storing request response in {path}...")
    with open(path, 'wb') as f:
        f.write(response.content)
    
    # ToDo: this part is related to STG (process/transform), is better to move in a stg function
    # Process and filter the request response to get the data related to execution_date
    raw_json = json.loads(response.content)    
    df = (
        pd.DataFrame(raw_json['Time Series (Daily)'])
        .T.reset_index()
        .rename(columns={'index': 'date','1. open':'open','2. high':'high',
        '3. low':'low','4. close':'close','5. volume':'volume'})
    )
    df = df[df['date'] == execution_date]
    if df.empty:
        df = pd.DataFrame(
            [[execution_date, np.nan, np.nan, np.nan, np.nan, np.nan]], 
            columns=['date', 'open', 'high', 'low', 'close', 'volume']
        )
    df['symbol'] = stock_ticker_symbol
    df = df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
    print(f"Resulting filtered data frame {df}...")

    return df.to_json()

def insert_daily_data_db(**context):
    import pandas as pd
    from datetime import datetime, timezone
    
    task_instance = context['ti']
    print(f"Task Instance: {task_instance}...")

    # ToDo: this part of code need to be redifined, using raw data instead of xcom data frame.
    dfs = []
    for stock_ticker_symbol in conf.stocks_symbols_list:
        print(f"Get xcom for upstream task get_daily_data_: {stock_ticker_symbol}...")
        stock_df = pd.read_json(
            task_instance.xcom_pull(task_ids=f'get_daily_data_{stock_ticker_symbol}'),
            orient='index',
        ).T        
        stock_df['date_time_dag_run'] = datetime.now(timezone.utc) # Used to validate daily_ticker_table consistency
        stock_df = stock_df[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'date_time_dag_run']]
        dfs.append(stock_df)
    df = pd.concat(dfs, axis=0)      
    print(f"Resulting data frame: {df}...") 
    
    sql_cli = PostgresqlClient(conf.stocks_conn_user, conf.stocks_conn_pass, conf.stocks_conn_host, conf.stocks_conn_port, conf.stocks_conn_db, conf.stocks_conn_schema)
    print(f"Sql client: {sql_cli}...")
    try:
        sql_cli.insert_from_frame(df, conf.stocks_conn_daily_ticker_table)
        print(f"Inserted {len(df)} records")
    except sqlalchemy.exc.IntegrityError:
        print("Integrity Error")

def perform_weekly_report(**context):
    from datetime import timedelta
    import plotly.express as px
    import pandas as pd

    execution_date = f"{context['execution_date']:%Y-%m-%d}"
    sql_cli = PostgresqlClient(conf.stocks_conn_user, conf.stocks_conn_pass, conf.stocks_conn_host, conf.stocks_conn_port, conf.stocks_conn_db, conf.stocks_conn_schema)
    sql = SQL_WEEKLY_REPORT.format(execution_date=execution_date)
    print(f"SQL executed {sql}")
    df = sql_cli.to_frame(sql)
    df['date'] = pd.to_datetime(df.date, format='%Y-%m-%d') 
    df['date'] = df['date'].dt.strftime('%a %d/%b') # To fix plotly autocomplete dates x axis
    print(f"Resulting DF {df}...")

    execution_weekstart = context['execution_date'] - timedelta(days=context['execution_date'].weekday())
    file_name = f"{execution_weekstart:%Y%m%d}" + f'_weekly_report.png'
    fig = px.line(df, x='date', y='avg_price', color='symbol', markers=True, title="Weekly avg price of stock symbols")
    fig.update_layout(xaxis=dict(tickformat="%a %d/%b"))
    fig.write_image(f"daily_stock_report/{file_name}")
# --------------------------------------------------------------------------------------------



default_args = {
    'owner': 'flanfranco',
    'retries': 0, 
    'start_date': days_ago(21),
}
with DAG(
    'stocks_etl_dag', 
    max_active_runs=1, # To handle api limits
    catchup=True,
    default_args=default_args, 
    schedule_interval='0 08 * * 1-5'
    ) as dag:

    create_schema_table_if_not_exists = PythonOperator(
        task_id='create_schema_table_if_not_exists',
        python_callable= create_schema_table_if_not_exists
        )

    get_data_task = {}
    for stock_ticker_symbol in conf.stocks_symbols_list:
        get_data_task[stock_ticker_symbol] = PythonOperator(
            task_id=f'get_daily_data_{stock_ticker_symbol}',
            retries=3, # To handle api connection issues
            python_callable= get_daily_stock_data_json,
            op_args=[stock_ticker_symbol],
        )

    insert_daily_data = PythonOperator(
        task_id='insert_daily_data', python_callable= insert_daily_data_db
    )

    do_weekly_report = PythonOperator(
        task_id='do_weekly_report', python_callable= perform_weekly_report
    )


    for stock_ticker_symbol in conf.stocks_symbols_list:
        upstream_task = create_schema_table_if_not_exists
        task = get_data_task[stock_ticker_symbol]
        upstream_task.set_downstream(task)
        task.set_downstream(insert_daily_data)
    insert_daily_data.set_downstream(do_weekly_report)