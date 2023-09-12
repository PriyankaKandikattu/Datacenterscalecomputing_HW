
import pandas as pd
import argparse

# Reading the data
def extract_data(source):
    return pd.read_csv(source)

# Cleaning the Data

def clean_data(data):
    new_data = data.copy()
    to_drop = ['Edition Statement',
           'Corporate Author',
           'Corporate Contributors',
           'Former owner',
           'Engraver',
           'Contributors',
           'Issuance type',
            'Shelfmarks']
    new_data.drop(to_drop, inplace=True, axis=1)
    #new_data['Date of Publication'] = pd.to_numeric(extr)
    return new_data

def load_data(data , target):
    data.to_csv(target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Running ......")

    df = extract_data(args.source)
    new_df = clean_data(df)
    load_data(new_df, args.target)
    print("Job Complete")







