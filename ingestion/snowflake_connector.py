import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

class SnowflakeConnector:
    def __init__(self,username,password,account,warehouse,database,schema):
        self.username = username
        self.password = password
        self.account = account
        self.warehouse=warehouse
        self.database = database
        self.schema = schema
        self.conn = None

    def auth(self):
        try:
            self.conn = snow.connect(
                user=self.username,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            print("Connection Successful")
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.conn = None

    def write(self,df,dfName):
        try:
            write_pandas(self.conn,df,dfName,auto_create_table=True,use_logical_type=True)
            print("Table written successfully")
        except Exception as e:
            print(f"Error: {e}")

    def ingest(self,upload_info):
        self.auth()
        for element in upload_info:
            name = element["name"]
            content = element["content"]
            try:
                self.write(content,name)
                print(f"Uploaded {name}")
            except Exception as e:
                print(f"Error uploading {name} : {e}")
        try:
            self.conn.close()
            print("Connection Closed")
        except Exception as e:
            print(f"Error Closing Connection: {e}")
    