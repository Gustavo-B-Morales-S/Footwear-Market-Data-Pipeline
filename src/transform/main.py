import sqlite3
import pandas as pd
from typing import List
from datetime import datetime
from pandas import DataFrame
from sqlite3 import Connection

df: DataFrame = pd.read_json(path_or_buf='../../data/data.jsonl', lines=True)

# Adding metadata columns
df['_collection_date'] = datetime.now()
df['_source'] = 'https://lista.mercadolivre.com.br/tenis-corrida-masculino'
df['reviews_amount'] = df['reviews_amount'].str.replace(pat='[\(\)]', 
                                                        repl='', 
                                                        regex=True)
# Setting data types
numeric_columns_dtypes: dict[str, type] = {
    'old_price_in_reais'   : float, 
    'old_price_in_cents'   : float,
    'new_price_in_reais'   : float, 
    'new_price_in_cents'   : float, 
    'reviews_rating_number': float, 
    'reviews_amount'       : int
}

# Getting column labels from dictionary keys
numeric_columns_labels: List[str] = list(numeric_columns_dtypes)

# Changing data types
df[numeric_columns_labels] = df[numeric_columns_labels].fillna(value=0) \
                                                       .astype(dtype=numeric_columns_dtypes)

# Creating new price columns from old/new price columns in reais and cents
df.insert(loc=2, column='old_price', value=(
    df['old_price_in_reais'] + df['old_price_in_cents'] / 100)
    )
df.insert(loc=3, column='new_price', value=(
    df['new_price_in_reais'] + df['new_price_in_cents'] / 100)
    )

# Removing previous price columns
df.drop(columns=numeric_columns_labels[0:4], inplace=True)
                
                                                
if __name__ == '__main__':
    connection: Connection = sqlite3.connect(database='../../data/quotes.db')

    df.to_sql(con=connection, 
              name='Mercado_Livre_Items', 
              if_exists='replace', 
              index=False)
    
    connection.close()
    
