# Native Libraries
import sqlite3
from datetime import datetime
from typing import Dict, List, Callable, Optional, Any

# Third-Party Libraries
import pandas as pd
from pandas import DataFrame


DATA_PATH: str = '../../data/sports_shoes_data.jsonl'
DATABASE_PATH: str = '../../data/quotes.db'
TABLE_NAME: str = 'Mercado_Livre_Items'
SOURCE_URL: str = 'https://lista.mercadolivre.com.br/tenis-corrida-masculino'


# Data type definitions for numeric columns
NUMERIC_COLUMNS_DTYPES: Dict[str, type] = {
    'old_price_in_reais': float,
    'old_price_in_cents': float,
    'new_price_in_reais': float,
    'new_price_in_cents': float,
    'reviews_rating_number': float,
    'reviews_amount': int
}


def load_raw_data(file_path: str) -> DataFrame:
    """
    Load raw JSONL data into a pandas DataFrame and add metadata columns.

    Args:
        file_path: Path to the JSONL data file

    Returns:
        pandas DataFrame containing the raw data with metadata
    """
    df: DataFrame = pd.read_json(path_or_buf=file_path, lines=True)
    df['_collection_date'] = datetime.now()
    df['_source'] = SOURCE_URL
    return df


def clean_reviews_amount(df: DataFrame) -> DataFrame:
    """
    Clean the reviews_amount column by removing parentheses.

    Args:
        df: DataFrame containing the raw data

    Returns:
        DataFrame with cleaned reviews_amount column

    Raises:
        AttributeError: If reviews_amount column is not of string type
    """
    if not pd.api.types.is_string_dtype(df['reviews_amount']):
        raise AttributeError("reviews_amount column must be of string type")

    return df.assign(
        reviews_amount=df['reviews_amount'].str.replace(
            pat=r'[\(\)]',
            repl='',
            regex=True
        )
    )


def convert_numeric_types(df: DataFrame) -> DataFrame:
    """
    Convert numeric columns to their appropriate data types.

    Args:
        df: DataFrame with raw data types

    Returns:
        DataFrame with proper numeric data types
    """
    return df.assign(
        **{
            col: pd.to_numeric(df[col].fillna(0), errors='coerce').astype(dtype)
            for col, dtype in NUMERIC_COLUMNS_DTYPES.items()
        }
    )


def create_combined_price_columns(df: DataFrame) -> DataFrame:
    """
    Create combined price columns from reais and cents components.

    Args:
        df: DataFrame with separate price columns

    Returns:
        DataFrame with combined price columns
    """
    return df.assign(
        old_price=df['old_price_in_reais'] + df['old_price_in_cents'] / 100,
        new_price=df['new_price_in_reais'] + df['new_price_in_cents'] / 100
    ).drop(columns=list(NUMERIC_COLUMNS_DTYPES.keys())[0:4])


def process_data(df: DataFrame) -> DataFrame:
    """
    Process the raw DataFrame through all cleaning and transformation steps.

    Args:
        df: Raw DataFrame from the JSONL file

    Returns:
        Fully processed DataFrame ready for database storage
    """
    processing_pipeline: List[Callable[[DataFrame], DataFrame]] = [
        clean_reviews_amount,
        convert_numeric_types,
        create_combined_price_columns
    ]

    for step in processing_pipeline:
        df = step(df)

    return df


def save_to_database(
    df: DataFrame,
    db_path: str,
    table_name: str,
    **kwargs: Optional[Any]
) -> None:
    """
    Save processed DataFrame to SQLite database.

    Args:
        df: Processed DataFrame
        db_path: Path to SQLite database file
        table_name: Name of the table to create/replace
        **kwargs: Additional arguments to pass to to_sql()

    Raises:
        sqlite3.Error: If there's an error during database operation
    """
    try:
        with sqlite3.connect(database=db_path) as conn:
            df.to_sql(
                con=conn,
                name=table_name,
                if_exists='replace',
                index=False,
                **kwargs
            )
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise


def main() -> None:
    """Main data processing pipeline."""
    try:
        # Load and process data
        raw_data: DataFrame = load_raw_data(DATA_PATH)
        processed_data: DataFrame = process_data(raw_data)

        # Save to database
        save_to_database(processed_data, DATABASE_PATH, TABLE_NAME)
        print(f"Data successfully processed and saved to {TABLE_NAME}")
    except Exception as e:
        print(f"Error in processing pipeline: {e}")
        raise


if __name__ == '__main__':
    main()
