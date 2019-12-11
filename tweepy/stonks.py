import datetime as dt
import pandas as pd
import pandas_datareader.data as web


start = dt.datetime(2018,1,1);
end = dt.datetime(2019,1,1);

df = web.DataReader('AAPL','yahoo',start,end);
print(df);