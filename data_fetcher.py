import yfinance as yf
import pandas as pd 

class DataFetcher:
	def __init__(self, symbols, start_date, end_date):

		"""Initialize the object with symbol start date and end date to fetch data"""

		self.symbols = symbols
		self.start_date = start_date
		self.end_date = end_date
		self.combined_data = []

	def fetch_data(self):

		"""
		This function is used to fetch the data of all the symbols and return the dataframe
		"""

		all_data = []

		for symbol in self.symbols:
			print("fetching the data for ",symbol)
			stock_data = yf.download(symbol, start = self.start_date, end = self.end_date)
			stock_data.reset_index(inplace=True)
			stock_data["stock_symbol"] = symbol
			stock_data.columns = ["date", "close", "high", "low", "open", "volume", "symbol"]
			all_data.append(stock_data)
		self.combined_data = pd.concat(all_data, ignore_index = True)
		print("Data fetching is complete")
		return self.combined_data
		






