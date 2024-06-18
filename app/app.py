# Thirdy Party Libraries
import streamlit as st
import pandas as pd
import sqlite3

from pandas import DataFrame
from sqlite3 import Connection

database_connection: Connection = sqlite3.connect(database='../data/quotes.db')

df = pd.read_sql_query(
    sql='SELECT * FROM Mercado_Livre_Items', 
    con=database_connection
    )

database_connection.close()


st.title('Pesquisa de Mercado - Tênis Esportivos no Mercado Livre')

st.subheader('KPIs principais do sistema')
col1, col2, col3 = st.columns(3)

# KPI 1: Número total de itens
total_itens = df.shape[0]
col1.metric(label='Número Total de Itens', value=total_itens)

# KPI 2: Número de marcas únicas
unique_brands = df['brand'].nunique()
col2.metric(label='Número de Marcas Únicas', value=unique_brands)

# KPI 3: Preço médio novo (em reais)
average_new_price = df['new_price'].mean()
col3.metric(label='Preço Médio Novo (R$)', value=f'{average_new_price:.2f}')


# Quais marcas são mais encontradas até a 20ª página
st.subheader('Marcas mais encontradas até a 20ª página')
col1, col2 = st.columns([4, 2])

top_20_pages_brands = df['brand'].value_counts() \
                                 .sort_values(ascending=False)
col1.bar_chart(top_20_pages_brands)
col2.write(top_20_pages_brands)

# Qual o preço médio por marca
st.subheader('Preço médio por marca')
col1, col2 = st.columns([4, 2])

df_non_zero_prices = df[df['new_price'] > 0]
average_price_by_brand = df_non_zero_prices.groupby('brand')['new_price'] \
                                           .mean() \
                                           .sort_values(ascending=False)
col1.bar_chart(average_price_by_brand)
col2.write(average_price_by_brand)

# Qual a satisfação por marca
st.subheader('Satisfação por marca')
col1, col2 = st.columns([4, 2])
df_non_zero_reviews = df[df['reviews_rating_number'] > 0]

satisfaction_by_brand = df_non_zero_reviews.groupby('brand')['reviews_rating_number']\
                                           .mean()\
                                           .sort_values(ascending=False)
col1.bar_chart(satisfaction_by_brand)
col2.write(satisfaction_by_brand)
