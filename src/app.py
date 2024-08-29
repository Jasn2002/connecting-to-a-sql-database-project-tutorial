import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

#DB Commands:

file_paths = ['src/sql/create.sql','src/sql/insert.sql', 'src/sql/drop.sql']

def execute_statements(*args) -> None:
    for arg in args:
        with open(arg, 'r') as file:
            sql_script = file.read()
        with engine.connect() as sql_db:
            for statement in sql_script.split(';'):
                statement = statement.strip()
                if statement:
                    sql_db.execute(text(statement))

def read_table(_table_name : str, conn) -> None:

    df = pd.read_sql( 
    _table_name, 
    con=engine)

    print(df.head())

# 1) Connect to the database here using the SQLAlchemy's create_engine function

connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(connection_string).execution_options(autocommit=True)

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

execute_statements(file_paths[0]) #This line creates tables

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

execute_statements(file_paths[1])#This line inserts into tables

# 4) Use pandas to print one of the tables as dataframes using read_sql function

read_table('books', engine)



execute_statements(file_paths[2])#This line drops tables, leaving a clean database