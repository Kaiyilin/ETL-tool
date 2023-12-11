import mysql.connector
from pymongo import MongoClient


class Extract:
    def __init__(self, conn_str, user_name, password, db, port="default") -> None:
        self.conn_str = conn_str
        self.user_name = user_name
        self.password = password
        self.db = db
        self.port = port

        # Auto-connect to the database
        self._connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._disconnect()

    def _connect(self):
        connection_support = {
            "mysql": mysql.connector.connect,
            "mongo": MongoClient,
        }

        try:
            # Validate port
            if not isinstance(self.port, int):
                raise ValueError("Port must be an integer.")

            # Determine connection function based on database type
            if self.db in connection_support.keys():
                connection_func = connection_support[self.db]
            else:
                raise ValueError(f"db must be one of: {connection_support.keys()}")

            # Connect to the database
            self.connection = connection_func(
                host=self.conn_str,
                user=self.user_name,
                password=self.password,
                port=self.port,
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    def _disconnect(self):
        try:
            self.connection.close()
        except Exception as e:
            print(f"Error disconnecting from database: {e}")

    def _get_sql_script(self, sql_script_path):
        try:
            with open(sql_script_path, "r") as f:
                sql_script = f.read()
            return sql_script
        except Exception as e:
            raise FileNotFoundError(f"Failed to read SQL script: {e}")

    def extract_from_db(self, sql_script_path):
        try:
            # Fetch the SQL script
            query_script = self._get_sql_script(sql_script_path)

            # Execute the query
            cursor = self.connection.cursor()
            cursor.execute(query_script)

            # Fetch results
            results = cursor.fetchall()

            return results
        except Exception as e:
            print(f"Error executing query: {e}")
            return []



# config = {
#     "conn_str" : "http://localhost",
#     "user_name" : "test",
#     "password" : "test",
#     "db" : "mysql",
#     "port" : 8888
# }

# extracting = Extract(**config)

# print(extracting)