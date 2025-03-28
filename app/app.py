# Native Libraries
import sqlite3

# Third-Party Libraries
from pathlib import Path
import streamlit as st
import pandas as pd


DATABASE_PATH = Path('../data/quotes.db')
TABLE_NAME = 'Mercado_Livre_Items'


def load_data_from_db(db_path: Path, table_name: str) -> pd.DataFrame:
    """
    Load data from SQLite database into a pandas DataFrame.

    Args:
        db_path: Path to the SQLite database file
        table_name: Name of the table to query

    Returns:
        pandas DataFrame containing the query results
    """
    try:
        with sqlite3.connect(database=str(db_path)) as conn:
            return pd.read_sql_query(
                sql=f'SELECT * FROM {table_name}',
                con=conn,
            )
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def display_kpi_metrics(df: pd.DataFrame) -> None:
    """
    Display key performance indicators in a 3-column layout.

    Args:
        df: DataFrame containing the market data
    """
    st.subheader('Key Performance Indicators')
    col1, col2, col3 = st.columns(3)

    # KPI 1: Total number of items
    total_items = df.shape[0]
    col1.metric(label='Total Items Scraped', value=total_items)

    # KPI 2: Number of unique brands
    unique_brands = df['brand'].nunique()
    col2.metric(label='Unique Brands Found', value=unique_brands)

    # KPI 3: Average new price (in BRL)
    average_new_price = df['new_price'].mean()
    col3.metric(label='Average Price (R$)', value=f'{average_new_price:.2f}')


def display_brand_distribution(df: pd.DataFrame) -> None:
    """
    Display brand distribution visualization and data.

    Args:
        df: DataFrame containing the market data
    """
    st.subheader('Brand Distribution (Top 20 Pages)')
    col1, col2 = st.columns([4, 2])

    brand_counts = df['brand'].value_counts().sort_values(ascending=False)
    col1.bar_chart(brand_counts, use_container_width=True)
    col2.dataframe(brand_counts, height=300)


def display_average_prices(df: pd.DataFrame) -> None:
    """
    Display average price by brand visualization.

    Args:
        df: DataFrame containing the market data
    """
    st.subheader('Average Price by Brand')
    col1, col2 = st.columns([4, 2])

    # Filter out items with zero/negative prices
    df_prices = df[df['new_price'] > 0]
    avg_price = df_prices.groupby('brand')['new_price'].mean().sort_values(ascending=False)

    col1.bar_chart(avg_price, use_container_width=True)
    col2.dataframe(avg_price, height=300)


def display_customer_satisfaction(df: pd.DataFrame) -> None:
    """
    Display customer satisfaction ratings by brand.

    Args:
        df: DataFrame containing the market data
    """
    st.subheader('Customer Satisfaction by Brand')
    col1, col2 = st.columns([4, 2])

    # Filter out items without reviews
    df_reviews = df[df['reviews_rating_number'] > 0]
    satisfaction = df_reviews.groupby('brand')['reviews_rating_number'] \
                            .mean() \
                            .sort_values(ascending=False)

    col1.bar_chart(satisfaction, use_container_width=True)
    col2.dataframe(satisfaction, height=300)


def main():
    """Main application function."""
    st.title('Market Research - Sports Tennis Shoes on Mercado Livre')

    # Load data
    df = load_data_from_db(DATABASE_PATH, TABLE_NAME)
    if df.empty:
        st.warning("No data available. Please check the database connection.")
        return

    # Display dashboard components
    display_kpi_metrics(df)
    display_brand_distribution(df)
    display_average_prices(df)
    display_customer_satisfaction(df)


if __name__ == '__main__':
    main()
