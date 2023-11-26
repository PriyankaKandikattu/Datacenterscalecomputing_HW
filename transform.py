import pandas as pd
import numpy as np
from collections import OrderedDict
from s3fs.core import S3FileSystem

# # creating the global mapping for outcome types
# outcomes_map = {'Rto-Adopt':1, 
#                 'Adoption':2, 
#                 'Euthanasia':3, 
#                 'Transfer':4,
#                 'Return to Owner':5, 
#                 'Died':6, 
#                 'Disposal':7,
#                 'Missing':8,
#                 'Relocate':9,
#                 'N/A':10,
#                 'Stolen':11}

# def transform_data(**kwargs):
#     ti = kwargs['ti']
#     df = ti.xcom_pull(task_ids='Extract')
#     new_data = df.copy()
#     new_data = prep_data(new_data)

#     dim_animal = prep_animal_dim(new_data)
#     dim_dates = prep_date_dim(new_data)
#     dim_outcome_types = prep_outcome_types_dim(new_data)

#     fct_outcomes = prep_outcomes_fct(new_data)

#     # Save dimension tables to S3
#     save_to_s3(dim_animal, 'dim_animals.csv')
#     save_to_s3(dim_dates, 'dim_dates.csv')
#     save_to_s3(dim_outcome_types, 'dim_outcome_types.csv')

#     # Save fact Tables to S3

#     save_to_s3(fct_outcomes,'fact_outcomes.csv')
    
#     # We'll use a dictionary so that we could get simultaneously table name and contents when using to_sql
#     # note that fact table can only be updated after dimensions have been updated
#     return OrderedDict({'dim_animals':dim_animal, 
#             'dim_dates':dim_dates,
#             'dim_outcome_types':dim_outcome_types,
#             'fct_outcomes':fct_outcomes
#             })


# def prep_data(data):
#     # remove stars from animal names. Need regex=False so that * isn't read as regex
#     data['name'] = data['name'].str.replace("*","",regex=False)

#     # separate the "sex upon outcome" column into property of an animal (male or female) 
#     # and property of an outcome (was the animal spayed/neutered at the shelter or not)
#     data['sex'] = data['sex_upon_outcome'].replace({"Neutered Male":"M",
#                                                     "Intact Male":"M", 
#                                                     "Intact Female":"F", 
#                                                     "Spayed Female":"F", 
#                                                     "Unknown":np.nan})

#     data['is_fixed'] = data['sex_upon_outcome'].replace({"Neutered Male":True,
#                                                         "Intact Male":False, 
#                                                         "Intact Female":False, 
#                                                         "Spayed Female":True, 
#                                                         "Unknown":np.nan})

#     # prepare the data table for introducing the date dimension
#     # we'll use condensed date as the key, e.g. '20231021'
#     # time can be a separate dimension, but here we'll keep it as a field
#     data['ts'] = pd.to_datetime(data.datetime)
#     data['date_id'] = data.ts.dt.strftime('%Y%m%d')
#     data['time'] = data.ts.dt.time

#     # prepare the data table for introducing the outcome type dimension:
#     # introduce keys for the outcomes
#     data['outcome_type_id'] = data['outcome_type'].fillna('N/A')
#     data['outcome_type_id'] = data['outcome_type_id'].replace(outcomes_map)

#     return data

# def prep_animal_dim(data):
    
#     # extract columns only relevant to animal dim
#     animal_dim = data[['animal_id','name','date_of_birth', 'sex', 'animal_type', 'breed', 'color']]
    
#     # rename the columns to agree with the DB tables
#     animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']
    
#     # drop duplicate animal records
#     return animal_dim.drop_duplicates()

# def prep_date_dim(data):
#     # use string representation as a key
#     # separate out year, month, and day
#     dates_dim = pd.DataFrame({
#         'date_id':data.ts.dt.strftime('%Y%m%d'),
#         'date':data.ts.dt.date,
#         'year':data.ts.dt.year,
#         'month':data.ts.dt.month,
#         'day':data.ts.dt.day,
#         })
#     return dates_dim.drop_duplicates()

# def prep_outcome_types_dim(data):
#     # map outcome string values to keys
#     outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
#     # keep only the necessary fields
#     outcome_types_dim.columns=['outcome_type', 'outcome_type_id']    
#     return outcome_types_dim


# def prep_outcomes_fct(data):
#     # pick the necessary columns and rename
#     outcomes_fct = data[["animal_id", 'date_id','time','outcome_type_id','outcome_subtype','is_fixed']]
#     return outcomes_fct.rename(columns={"animal_id":"animal_id"})

# def save_to_s3(data, file_name):
#      # Upload the CSV file to S3
#     s3_bucket = "yamini-airflow-storage"
#     s3_path = f"s3://{s3_bucket}/{file_name}"

#     # AWS credentials
#     aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
#     aws_secret_access_key = "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"


#     # Save to S3
#     fs = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
#     with fs.open(s3_path, 'w') as file:
#         data.to_csv(file, index=False)

#     print(f"Saved {file_name} to S3.")



def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='Extract')
    new_data = df.copy()
    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)

    df_fact = new_data.merge(dim_dates, how='inner', left_on='date_recorded', right_on='date_recorded')
    df_fact = df_fact.merge(dim_animal, how='inner', left_on='animal_id', right_on='animal_id')
    df_fact = df_fact.merge(dim_outcome_types, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key',
        'outcome_count': 'outcome_count'
    }, inplace=True)


    # Create or append data to the Outcomes_Fact table
    fct_outcomes=df_fact[['date_key', 'animal_key', 'outcome_type_key']]


    # Save dimension tables to S3
    save_to_s3(dim_animal, 'dim_animals.csv')
    save_to_s3(dim_dates, 'dim_dates.csv')
    save_to_s3(dim_outcome_types, 'dim_outcome_types.csv')

    # Save fact Tables to S3

    save_to_s3(fct_outcomes,'fact_outcomes.csv')
    
    # We'll use a dictionary so that we could get simultaneously table name and contents when using to_sql
    # note that fact table can only be updated after dimensions have been updated
    return OrderedDict({'dim_animals':dim_animal, 
            'dim_dates':dim_dates,
            'dim_outcome_types':dim_outcome_types,
            'fct_outcomes':fct_outcomes
            })

# A function to convert age to years
def age_to_years(age):
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

def map_month_to_quarter(month):
    if 1 <= month <= 3:
        return 'Q1'
    elif 4 <= month <= 6:
        return 'Q2'
    elif 7 <= month <= 9:
        return 'Q3'
    else:
        return 'Q4'

def prep_data(data):
    transformed_data = data.copy()
    #transformed_data[['month_recorded', 'year_recorded']] = transformed_data['monthyear'].str.split(' ', expand=True)
    transformed_data['monthyear']= pd.to_datetime(transformed_data['monthyear'])
    # Extract month and year
    transformed_data['month_recorded'] = transformed_data['monthyear'].dt.month
    transformed_data['year_recorded'] = transformed_data['monthyear'].dt.year
    transformed_data['date_of_birth'] = pd.to_datetime(transformed_data['date_of_birth'])
    # Format the datetime object to the desired format
    transformed_data['date_of_birth']= transformed_data['date_of_birth'].dt.strftime('%m/%d/%Y')
    transformed_data['animal_name'] = transformed_data['name'].fillna('Name_less')
    transformed_data['Sex upon Outcome'] = transformed_data['sex_upon_outcome'].fillna('Unknown')
    transformed_data['Sex upon Outcome'] = transformed_data['Sex upon Outcome'].replace('Unknown', 'Unknown Unknown')
    transformed_data[['sterilization_status', 'gender']] = transformed_data['Sex upon Outcome'].str.split(' ', expand=True)
    transformed_data['Age upon Outcome'] = transformed_data['age_upon_outcome'].astype(str)
    transformed_data['age_years'] = transformed_data['Age upon Outcome'].apply(age_to_years)
    transformed_data.drop(columns = ['monthyear','name', 'Sex upon Outcome', 'Age upon Outcome', 'outcome_subtype'], axis=1, inplace=True)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['datetime'])
    transformed_data['day_of_week'] = transformed_data['DateTime'].dt.day_name()
    transformed_data['quarter_recorded'] = transformed_data['DateTime'].dt.month.apply(map_month_to_quarter)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['DateTime'])
    transformed_data['DateTime'] = transformed_data['DateTime'].dt.date
    cols_mapping = {
    'animal_id': 'animal_id',
    'datetime': 'date_recorded',
    'date_of_birth': 'dob'
    }
    transformed_data.rename(columns=cols_mapping, inplace=True)
    return transformed_data


def prep_outcome_types_dim(new_data):
    outcome_type_dim_data = new_data[['outcome_type']].drop_duplicates()
    outcome_type_dim_data['outcome_type_key'] = range(1, len(outcome_type_dim_data) + 1)
    cols = ['outcome_type_key'] + [col for col in outcome_type_dim_data if col != 'outcome_type_key']
    outcome_type_dim_data = outcome_type_dim_data[cols]
    return outcome_type_dim_data

def prep_animal_dim(new_data):
    animal_dim_data = new_data[['animal_id', 'animal_name', 'dob', 'animal_type', 'sterilization_status', 'gender', 'age_years', 'breed', 'color']].drop_duplicates()
    animal_dim_data['animal_key'] = range(1, len(animal_dim_data) + 1)
    # Reorder columns to move animal_key to the beginning
    cols = ['animal_key'] + [col for col in animal_dim_data if col != 'animal_key']
    animal_dim_data = animal_dim_data[cols]
    return animal_dim_data

def prep_date_dim(new_data):
    date_dim_data = new_data[['date_recorded','day_of_week', 'month_recorded', 'quarter_recorded', 'year_recorded']].drop_duplicates()
    date_dim_data['date_key'] = range(1, len(date_dim_data) + 1)
    cols = ['date_key'] + [col for col in date_dim_data if col != 'date_key']
    date_dim_data = date_dim_data[cols]
    return date_dim_data

def save_to_s3(data, file_name):
     # Upload the CSV file to S3
    s3_bucket = "yamini-airflow-storage"
    s3_path = f"s3://{s3_bucket}/{file_name}"
    # AWS credentials
    # AWS credentials
    aws_access_key_id = "AKIAZUV3WBBACPMQFACE"
    aws_secret_access_key = "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"
    # Save to S3
    fs = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    with fs.open(s3_path, 'w') as file:
        data.to_csv(file, index=False)

    print(f"Saved {file_name} to S3.")