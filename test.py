import pandas as pd 
import yfinance as yf 

data = yf.download("AAPL", start = "2025-01-01", end = "2025-05-01", auto_adjust = False)

print(data.head())