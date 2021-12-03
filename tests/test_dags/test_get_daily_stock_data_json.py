from  dags.stocks_etl_dag import get_daily_stock_data_json 
from datetime import datetime, timedelta
import pandas as pd 
import pandas.api.types as ptypes

# Testing Date (Weekstart)
dt = datetime.now()
execution_weekstart = dt - timedelta(days = dt.weekday())

def test_get_daily_stock_data_json():
    # Getting df using DAG function    
    result = get_daily_stock_data_json('AMZN', execution_date = execution_weekstart)
    df_to_check = pd.read_json(
            result,
            orient='index',
        ).T
    
    # Casting to expected data types 
    num_cols = ['open', 'high', 'low', 'close', 'volume']
    for c in num_cols:
        try:
            df_to_check[c] = pd.to_numeric(df_to_check[c])
        except:
            pass
    try:
        df_to_check['date'] = pd.to_datetime(df_to_check['date'],format='%Y-%m-%d', errors='raise')
    except:
        pass

    # Testing
    assert all(ptypes.is_numeric_dtype(df_to_check[col]) for col in num_cols)
    assert ptypes.is_string_dtype(df_to_check['symbol'])
    assert ptypes.is_datetime64_any_dtype(df_to_check['date'])



