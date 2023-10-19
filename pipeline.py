import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine

def extract_data(source):
    return pd.read_csv(source)

def Convert_agetoyears(age):
    age_in_years = 0
    if isinstance(age, str):
        if 'year' in age:
            age_in_years = int(age.split()[0])
        elif 'month' in age:
            age_in_months = int(age.split()[0])
            age_in_years = age_in_months/12
        elif 'day' in age:
            age_in_days = int(age.split()[0])
            age_in_years = age_in_days/365
        

    if age_in_years < 1:
      age_in_years = 0

    return str(age_in_years)

def month_to_quarter(month):
    if 1 <= month <= 3:
        return 'Q1'
    elif 4 <= month <= 6:
        return 'Q2'
    elif 7 <= month <= 9:
        return 'Q3'
    else:
        return 'Q4'

def transform_data(data):
    new_data = data.copy()
    new_data = data.copy()
    new_data[['month', 'year']] = new_data['MonthYear'].str.split(' ', expand=True)
    new_data[['animal_name']] = new_data[['Name']].fillna('Name_less')
    new_data['Sex upon Outcome'] = new_data['Sex upon Outcome'].fillna('Unknown')
    new_data['Sex upon Outcome'] = new_data['Sex upon Outcome'].replace('Unknown', 'Unknown Unknown')
    new_data[['neuter_status', 'sex']] = new_data['Sex upon Outcome'].str.split(' ', expand=True)
    new_data['Age upon Outcome'] = new_data['Age upon Outcome'].astype(str)
    new_data['age_years'] = new_data['Age upon Outcome'].apply(Convert_agetoyears)
    new_data.drop(columns = ['MonthYear', 'Name', 'Sex upon Outcome', 'Age upon Outcome', 'Outcome Subtype'], axis=1, inplace=True)
    new_data['DateTime'] = pd.to_datetime(new_data['DateTime'])
    new_data['day_of_week'] = new_data['DateTime'].dt.day_name()
    new_data['quarter'] = new_data['DateTime'].dt.month.apply(month_to_quarter)
    new_data['DateTime'] = new_data['DateTime'].dt.date
    cols_mapping = {
    'Animal ID': 'animal_id',
    'DateTime': 'date_',
    'Date of Birth': 'date_of_birth',
    'Outcome Type': 'outcome_type',
    'Animal Type': 'animal_type',
    'Breed': 'breed',
    'Color': 'color'
    }
    new_data.rename(columns=cols_mapping, inplace=True)

    return new_data

def prepare_dim_animal_data(new_data):
    dim_animal_data = new_data[['animal_id', 'animal_name', 'date_of_birth', 'animal_type', 'neuter_status', 'sex', 'age_years', 'breed']].drop_duplicates()
    dim_animal_data['animal_key'] = range(1, len(dim_animal_data) + 1)
    return dim_animal_data

def prepare_dim_datetime_data(new_data):
    dim_datetime_data = new_data[['date_','day_of_week', 'month', 'quarter', 'year']].drop_duplicates()
    dim_datetime_data['date_id'] = range(1, len(dim_datetime_data) + 1)
    return dim_datetime_data

def prepare_dim_outcome_type_data(new_data):
    dim_outcome_type_data = new_data[['outcome_type']].drop_duplicates()
    dim_outcome_type_data['outcome_id'] = range(1, len(dim_outcome_type_data) + 1)
    return dim_outcome_type_data




def load_data(new_data):

    dim_animal_data = prepare_dim_animal_data(new_data)
    dim_datetime_data = prepare_dim_datetime_data(new_data)
    dim_outcome_type_data = prepare_dim_outcome_type_data(new_data)

    db_url = "postgresql+psycopg2://postgres:Yamini27@db:5432/shelter"
    conn = create_engine(db_url)
    dim_animal_data.to_sql("dim_animal", conn, if_exists="append", index=False)
    dim_datetime_data.to_sql("dim_datetime", conn, if_exists="append", index=False)
    dim_outcome_type_data.to_sql("dim_outcome_type", conn, if_exists="append", index=False)

    # Create or append data to the Outcomes_Fact table, linking to dimension tables
    df_fact = new_data.merge(dim_datetime_data, how='inner', left_on='date_', right_on='date_')
    df_fact = df_fact.merge(dim_animal_data, how='inner', left_on='animal_id', right_on='animal_id')
    df_fact = df_fact.merge(dim_outcome_type_data, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_id': 'date_id',
        'animal_key': 'animal_key',
        'outcome_id': 'outcome_id',
        'outcome_count': 'outcome_count'
    }, inplace=True)


    # Create or append data to the Outcomes_Fact table
    df_fact[['date_id', 'animal_key', 'outcome_id']].to_sql('fact_table_outcomes', conn, if_exists='append', index=False)


if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    #parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    new_df = transform_data(df)
    load_data(new_df)
    print("Complete")

    


