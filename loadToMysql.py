from sqlalchemy import create_engine
import pandas as pd 

class LoadToMysql:

	"""
	This class will load the data to the mysql database
	A user must pass the hostname, username, passowrd and database to build a connection

	"""

	def __init__(self, host, user, password, database):
		self.engine = self.build_connection(host, user, password, database)
		print(f"Connection built Successfully to {database}")


	def build_connection(self, host, user, password, database):

		"""
		This will return a connection to the __init__ function
		"""

		return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

	def load_to_mysql(self, data, table):

		"""
		This function will load the data to the mysql table. The user need to pass dataframe
		and the table name from the database.
		"""
		data.to_sql(
			name = table,
			con = self.engine,
			if_exists = "append",
			index = False)

		print(f"Successfully added the data to the {table}")

	def query_to_dataframe(self, query):

		df = pd.read_sql(query, con=self.engine)
		return df
