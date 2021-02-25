import pandas as pd 
import requests
import json
import datetime



def build_url(ticker, start_date = None, end_date = None, interval = "1d"):
    
    if end_date is None:  
        end_seconds = int(pd.Timestamp("now").timestamp())   
    else:
        end_seconds = int(pd.Timestamp(end_date).timestamp())
        
    if start_date is None:
        start_seconds = 7223400       
    else:
        start_seconds = int(pd.Timestamp(start_date).timestamp())
    
    base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
    site = base_url + ticker
    params = {"period1": start_seconds, "period2": end_seconds,
              "interval": interval.lower(), "events": "div,splits"}
    
    return site, params
  
#modified from https://github.com/atreadw1492/yahoo_fin/blob/master/yahoo_fin/stock_info.py
def get_data(ticker, start_date = None, end_date = None, index_as_date = True,
             interval = "1d"):
    '''Downloads historical stock price data into a pandas data frame.  Interval
       must be "1d", "1wk", "1mo", or "1m" for daily, weekly, monthly, or minute data.
       Intraday minute data is limited to 7 days.
    
       @param: ticker
       @param: start_date = None
       @param: end_date = None
       @param: index_as_date = True
       @param: interval = "1d"
    '''
    
    if interval not in ("1d", "1wk", "1mo", "1m"):
        raise AssertionError("interval must be of of '1d', '1wk', '1mo', or '1m'")
    
    # build and connect to URL
    site, params = build_url(ticker, start_date, end_date, interval)
    resp = requests.get(site, params = params)
    
    #if bad url, return 0
    if not resp.ok:
        return 0 
        
    
    # get JSON response
    data = resp.json()
    
    # get open / high / low / close data
    frame = pd.DataFrame(data["chart"]["result"][0]["indicators"]["quote"][0])
    if frame.shape[0]==0:
        return 0
    
    # get the date info
    try:
        temp_time = data["chart"]["result"][0]["timestamp"]
    except KeyError:
        return 0 
            
    if interval != "1m":
    
        # add in adjclose
        frame["adjclose"] = data["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]   
        frame['day'] = pd.to_datetime(temp_time, unit = "s")
        frame['day'] = frame.day.map(lambda dt: dt.floor("d"))
        frame = frame[["day", "open", "high", "low", "close", "adjclose", "volume"]]
            
    else:

        frame.index = pd.to_datetime(temp_time, unit = "s")
        frame = frame[["open", "high", "low", "close", "volume"]]
        
    return frame
