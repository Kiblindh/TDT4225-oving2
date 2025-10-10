import pandas as pd
import ast


def read_porto_csv(filepath='porto/porto/porto.csv'):
    """
    Reads the porto.csv file and returns a pandas DataFrame.
    """
    df = pd.read_csv(filepath)
    return df

df = read_porto_csv()

# Drop invalid trips if wanted
df_clean = df[df['POLYLINE'].apply(lambda x: len(ast.literal_eval(x)) >= 3)].copy()
df_clean.reset_index(drop=True, inplace=True)
df_clean.to_csv('porto/porto/porto_clean.csv', index=False)