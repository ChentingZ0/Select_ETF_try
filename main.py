tmp = yf.download(tickers=['SPY'], start=start_date, end=end_date, interval='1d')['Adj Close'].pct_change() + 1
# second_column = tmp.iloc[:, 1]
print(tmp)