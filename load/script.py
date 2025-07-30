import pandas as pd
from sqlalchemy import create_engine

# Define the database connection parameters
db_user = 'postgres'
db_password = 'Warren%40postgres'
db_host = '172.20.16.139'
db_port = '5432'
db_name = 'covid19_etl'

# create database connection engine
try:
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
except Exception as e:
    print(f"Error creating database engine: {e}")
    exit(1)

# paths to the CSV files
confirmed_cases_path = '/home/warren/covid19_ETL_project/data_library/cleaned_confirmed_cases_global.csv'
deaths_path = '/home/warren/covid19_ETL_project/data_library/cleaned_confirmed_deaths_global.csv'
recovered_cases_path = '/home/warren/covid19_ETL_project/data_library/cleaned_confirmed_recovered_global.csv'

# load the CSV files into pandas DataFrames
try:
    confirmed_cases_df = pd.read_csv(confirmed_cases_path)
    confirmed_deaths_df = pd.read_csv(deaths_path)
    recovered_cases_df = pd.read_csv(recovered_cases_path)
except FileNotFoundError as e:
    print(f"Error loading CSV files: {e}")
    exit(1)

# loading the data into the database
try:
    confirmed_cases_df.to_sql('confirmed_cases', engine, if_exists='replace', index=False)
    confirmed_deaths_df.to_sql('confirmed_deaths', engine, if_exists='replace', index=False)
    recovered_cases_df.to_sql('recovered_cases', engine, if_exists='replace', index=False)
    print("Data loaded successfully into the database.")
except Exception as e:
    print(f"Error loading data into the database: {e}")
    exit(1)