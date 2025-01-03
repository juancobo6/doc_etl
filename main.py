import logging

import pandas as pd

from doc_etl import extract, transform, insert


# Example ETL pipeline
@extract(["BICI_DM_EVO_CANAL_VENTA", "BICI_TH_EVO_FEST_MUNICIPIO"])
def read_multiple_csv_file(file_path):
    """Read multiple CSV files and return a pandas DataFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, None],
    }

    alt_data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300],
    }

    return pd.DataFrame(data), pd.DataFrame(alt_data)


@extract("BICI_DM_EVO_CANAL_VENTA")
def read_single_csv_file(file_path):
    """Read a CSV file and return a pandas DataFrame."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, None],
    }

    return pd.DataFrame(data)


# @extract(["BICI_DM_EVO_CANAL_VENTA", "BICI_TH_EVO_FEST_MUNICIPIO"])
def read_combined(file_path):
    """"""
    df = read_single_csv_file(file_path)
    df2, df3 = read_multiple_csv_file(file_path)

    return df, df2, df3


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
    df.loc[:, "new_value"] = df["value"] * 2
    return df


@insert("BICI_DM_EVO_SOPORTE")
def save_to_csv(df, output_file):
    """Save the DataFrame to a CSV file."""
    # df.to_csv(output_file, index=False)
    logging.info(f"Data saved to {output_file}")


@insert("BICI_DM_EVO_SECUENCIA")
def save_to_alt_csv(df, output_file):
    """Save the DataFrame to a CSV file."""
    # df.to_csv(output_file, index=False)
    logging.info(f"Data saved to {output_file}")


if __name__ == "__main__":
    # Step 1: Extract
    # mult_data_1, mult_data_2 = read_multiple_csv_file("data.csv")
    # raw_data = read_single_csv_file("data.csv")
    raw_data, mult_data_1, mult_data_2 = read_combined("data.csv")

    # Step 2: Transform
    clean_data, dirty_data = clean_data(raw_data)
    transformed_data = add_column(raw_data)

    # Step 3: Load
    save_to_csv(clean_data, "clean_data.csv")
    save_to_csv(dirty_data, "dirty_data.csv")
    save_to_alt_csv(transformed_data, "output.csv")

    save_to_csv(mult_data_1, "output2.csv")
    save_to_alt_csv(mult_data_2, "output3.csv")
