import pandas as pd
from sqlalchemy import create_engine
import json 
import codecs
import datetime 

with open("DCJWirrimbirra\Python\DB_settings.json") as j:
     config =  json.load(j)

def get_expiry_date():
    return datetime.datetime(9999,12,31)

def get_insert_date():
    return datetime.datetime.today()

def get_engine():
    url = f"mssql+pyodbc://{config['User']}:{config['Password']}@{config['Host']}:{config['Port']}/{config['DataBase']}?driver=SQL+Server"
    eng =create_engine(url)
    return eng

def get_source(source_file_path):
    return pd.read_csv(source_file_path,',')

def load_data_from_csv(source_file_path,table_name,schema,type="append"): 
     df_source = get_source(source_file_path)
     df_source["InsertDate"]= get_insert_date()
     df_source["InsertDate"] = get_expiry_date()
     df_source["MetaSource"] = source_file_path
     eng = get_engine()
     df_source.to_sql(table_name,eng,index=True,if_exists=type, schema=schema)

if __name__ == "__main__":
    file_path = r'C:\Users\murrayb3\Documents\Old Power Bi Data\Shift Fact Table.csv'
    load_data_from_csv(file_path,"FactShift","Hist")
