import logging

import pandas as pd

from etl_decorators import extract, transform, insert, write_json, write_mermaid, write_prompt


# Example ETL pipeline
@extract
def read_multiple_csv_file(file_path):
    """Read multiple CSV files and return a pandas DataFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, None]
    }

    alt_data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300]
    }

    return pd.DataFrame(data), pd.DataFrame(alt_data)

@extract("BICI_DM_EVO_CANAL_VENTA")
def read_single_csv_file(file_path):
    """Read a CSV file and return a pandas DataFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, None]
    }

    return pd.DataFrame(data)


@transform()
def clean_data(df):
    """Remove rows with missing values from the DataFrame. And return the cleaned DataFrame and the
    DataFrame with rows with missing values.
    """
    return df.dropna(), df[df.isnull().any(axis=1)]


@transform()
def add_column(df):
    """Add a new column 'new_value' to the DataFrame."""
    df = df.copy()  # Avoid SettingWithCopyWarning
    df.loc[:, 'new_value'] = df['value'] * 2
    return df


@insert("BICI_DM_EVO_SOPORTE")
def save_to_csv(df, output_file):
    """Save the DataFrame to a CSV file."""
    df.to_csv(output_file, index=False)
    logging.info(f"Data saved to {output_file}")


if __name__ == "__main__":
    # Step 1: Extract
    raw_data = read_single_csv_file("data.csv")

    # Step 2: Transform
    clean_data, dirty_data = clean_data(raw_data)
    transformed_data = add_column(raw_data)

    # Step 3: Load
    save_to_csv(clean_data, "clean_data.csv")
    save_to_csv(dirty_data, "dirty_data.csv")
    save_to_csv(transformed_data, "output.csv")

    write_json()
    write_mermaid()
    write_prompt()
    print("ETL process completed. Check 'etl_process' for details.")
