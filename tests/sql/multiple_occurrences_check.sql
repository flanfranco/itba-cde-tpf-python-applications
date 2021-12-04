select symbol, date, count(date_time_dag_run) as qty
from {stocks_conn_schema}.{stocks_conn_daily_ticker_table}
group by 
	symbol, date
having
	count(date_time_dag_run) > 1
;