import pandas as pd 
import yfinance as yf 

symbols = ["AAPL", "AMZN", "MSFT"]

all_data = []

for symbol in symbols:
	print("fetching the data for ",symbol)
	stock_data = yf.download(symbol, start = "2000-01-01", end = "2025-05-01", auto_adjust = False)
	stock_data.reset_index(inplace=True)
	stock_data["stock_symbol"] = symbol
	stock_data.drop(columns=["Adj Close"], inplace=True)
	stock_data.columns = ["date", "close", "high", "low", "open", "volume", "symbol"]
	all_data.append(stock_data)

final_df = pd.concat(all_data, ignore_index=True)
final_df.to_csv("stock_data.csv", index=False)
