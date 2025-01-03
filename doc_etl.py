import atexit
import functools
import inspect
import json
import os

import pandas as pd
import polars as pl

# TODO: Find a better way to handle getting the table schema
from const import data_schema as DB_SCHEMA


def parse_schema(schema):
    """
    Parse the schema of a table from the output of the defined query
    :param schema: A list with the schema of the table
    :return: A list with the columns of the table
    """
    columns = []
    for column in schema:
        column_name = column[2]
        columns.append(column_name)
    return columns


def get_table(table):
    """
    Get the schema of a table from the database schema
    :param table: A string with the table name
    :return: A dictionary with the table schema
    """
    database_dict = {table: []}
    for row in DB_SCHEMA:
        table_name = row[1]
        if table_name != table:
            continue
        database_dict[table_name].append(row)

    for table_name, table_schema in database_dict.items():
        table = {table_name: parse_schema(table_schema)}
    return table


# TODO: Is there a way to not use a global variable?
INFO_DICT = {}


def log_dataframe_info(df):
    """
    Get a string representation of the DataFrame
    :param df: DataFrame to log
    :return: A dictionary with the DataFrame label and columns
    """
    label = f"DataFrame_{id(df)}"

    if isinstance(df, pd.DataFrame):
        return {label: df.columns.tolist()}
    elif isinstance(df, pl.DataFrame):
        return {label: df.columns}
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df).__name__}")


def extract(extract_table):
    """
    Decorator to log information about the function
    :param extract_table: A string or list of strings with the table names
    :return: Decorator
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the function
            result = func(*args, **kwargs)

            # Get the function ID and the arguments ID to obtain an unique ID
            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            # Get information about the function
            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "extract"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            # Log the tables where the data is extracted
            # TODO: Find a better way to handle getting the table schema
            INFO_DICT[function_id]["db_table"] = []
            if isinstance(extract_table, (list, tuple)):
                for table in extract_table:
                    INFO_DICT[function_id]["db_table"].append(get_table(table))
            elif isinstance(extract_table, str):
                INFO_DICT[function_id]["db_table"].append(get_table(extract_table))

            # Log the returned DataFrames
            if isinstance(result, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["output"].append(log_dataframe_info(result))
            elif isinstance(result, (tuple, list)):
                for i, item in enumerate(result):
                    if isinstance(item, (pd.DataFrame, pl.DataFrame)):
                        INFO_DICT[function_id]["output"].append(
                            log_dataframe_info(item)
                        )

            return result

        return wrapper

    return decorator


def transform():
    """
    Decorator to log information about the function
    :return: Decorator
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get the function ID and the arguments ID to obtain an unique ID
            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            # Get information about the function
            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "transform"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["db_table"] = []
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            # Log the input DataFrames
            for i, arg in enumerate(args):
                if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["input"].append(log_dataframe_info(arg))

            # Execute the function
            result = func(*args, **kwargs)

            # Log the returned DataFrames
            if isinstance(result, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["output"].append(log_dataframe_info(result))
            elif isinstance(result, (tuple, list)):
                for i, item in enumerate(result):
                    if isinstance(item, (pd.DataFrame, pl.DataFrame)):
                        INFO_DICT[function_id]["output"].append(
                            log_dataframe_info(item)
                        )

            return result

        return wrapper

    return decorator


def insert(insert_table):
    """
    Decorator to log information about the function
    :param insert_table: A string or list of strings with the table names
    :return: Decorator
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get the function ID and the arguments ID to obtain an unique ID
            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            # Get information about the function
            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "insert"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            # Log the input DataFrames
            for i, arg in enumerate(args):
                if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["input"].append(log_dataframe_info(arg))

            # Execute the function
            result = func(*args, **kwargs)

            # Log the tables where the data is inserted
            # TODO: Find a better way to handle getting the table schema
            INFO_DICT[function_id]["db_table"] = []
            if isinstance(insert_table, (list, tuple)):
                for table in insert_table:
                    INFO_DICT[function_id]["db_table"].append(get_table(table))
            elif isinstance(insert_table, str):
                INFO_DICT[function_id]["db_table"].append(get_table(insert_table))

            return result

        return wrapper

    return decorator


def write_json(info_dict, path="doc_etl/raw.json"):
    """
    Write the dictionary to a JSON file
    :param info_dict: A dictionary with the information
    :return: None
    """
    with open(path, "w") as f:
        json.dump(info_dict, f, indent=4)


def write_mermaid(info_dict, path="doc_etl/mermaid.md"):
    """
    Write the dictionary to a Mermaid diagram
    :param info_dict: A dictionary with the information
    :param path: A string with the path to save the file
    :return: None
    """

    string = """
```mermaid
graph TD

    """

    for func_id, func_info in info_dict.items():
        tables = func_info.get("db_table", [])
        for table in tables:
            for table_name, columns in table.items():
                columns_string = "\n" + "\n".join([f"{column}" for column in columns])
                label_columns = "_" + str(table_name) + "_" + str(columns_string)
                label_string = str(table_name) + f'[("`{label_columns}`")]'

                if func_info["type"] == "extract":
                    string += f"{label_string} --> {func_id}[/{func_info['name']}/]\n"
                elif func_info["type"] == "insert":
                    string += f"{func_id}[/{func_info['name']}/] --> {label_string}\n"

        for input_df in func_info["input"]:
            for label, info in input_df.items():
                # If the number of columns is greater than 4, only show the first 3 and the last one
                if len(info) > 4:
                    columns_string = "\n".join(info[:3]) + "\n(...)\n " + info[-1]
                else:
                    columns_string = "\n" + "\n ".join(info)

                label_columns = "_" + str(label) + "_" + columns_string
                label_string = (
                    str(label)
                    + "@"
                    + '{ shape: braces, label: "'
                    + label_columns
                    + '" }'
                )
                string += f"{label_string} --> {func_id}[/{func_info["name"]}/]\n"

        for output_df in func_info["output"]:
            for label, info in output_df.items():
                # If the number of columns is greater than 4, only show the first 3 and the last one
                if len(info) > 4:
                    columns_string = "\n".join(info[:3]) + "\n(...)\n " + info[-1]
                else:
                    columns_string = "\n" + "\n ".join(info)

                label_columns = "_" + str(label) + "_" + columns_string
                label_string = (
                    str(label)
                    + "@"
                    + '{ shape: braces, label: "'
                    + label_columns
                    + '" }'
                )
                string += f"{func_id}[/{func_info["name"]}/] --> {label_string}\n"

    string += "```"

    with open(path, "w") as f:
        f.write(string)


def convert_df_to_string(dfs):
    """
    Convert a list of DataFrames to a string
    :param dfs: A list of DataFrames
    :return: A string with the DataFrames
    """
    return_string = ""
    for dict_df in dfs:
        for table_name, columns in dict_df.items():
            columns_string = (
                "\n - " + "\n - ".join([f"{column}" for column in columns]) + "\n"
            )
            return_string += f"{table_name}: {columns_string}\n"

    return return_string


def write_intro_prompt(info_dict):
    """
    Write the prompt for the introduction section
    :param info_dict: A dictionary with the information
    :return: A string with the prompt
    """

    prompt = """Please redact the introduction to the documentation for this ETL process, focusing on the different relationships between the various executions of the functions. This section should only be an introduction, as there will be specific sections made for extraction, transformations and insertions. The response should be structured with clear sections and titles, with an overall overview of the whole ETL process. The overall tone should be narrative, with long sentences that explain the flow and interconnectedness of the functions. Ensure that the output reflects the complexity of the process while being organized and easy to follow. The documentation should include:

- An overview of the ETL process, summarizing its purpose and main stages.
- A clear explanation of the flow between the functions, detailing how data moves through the process from extraction to final output.
- A conclusion that ties together the entire process, emphasizing the role each function plays in achieving the final result.

Here you have a brief description of each function used, with the type of functions (extract, transform or insert),  the name of the function, the docstring of function, and the input and output DataFrames or database tables for each one:\n\n"""

    # Iterate over the functions to get only the information needed
    for func_id, func_info in info_dict.items():
        input_key = "input"
        output_key = "output"
        if func_info["type"] == "extract":
            input_key = "db_table"
        elif func_info["type"] == "insert":
            output_key = "db_table"

        prompt += f"Type: {func_info["type"]}\n\n"
        prompt += f"Function: {func_info["name"]}\n\n"
        prompt += f"Docstring: {func_info["docstring"]}\n\n"
        prompt += f"Input DataFrames:\n{convert_df_to_string(func_info[input_key])}\n\n"
        prompt += (
            f"Output DataFrames:\n{convert_df_to_string(func_info[output_key])}\n\n"
        )

    return prompt


def write_process_prompt(info_dict, process):
    """
    Write the prompt for a specific process section
    :param info_dict: A dictionary with the information
    :param process: A string with the process type
    :return: A string with the prompt
    """

    prompt = f"""Please redact the documentation for the {process} part in this ETL process, focusing on the different relationships between the various executions of the functions. The response should be structured with clear sections and titles, each addressing specific aspects of the {process} process. The overall tone should be narrative, with long sentences that explain the flow and interconnectedness of the functions. Ensure that the output reflects the complexity of the process while being organized and easy to follow. The documentation should include:

- A detailed description of each function in the {process} process, emphasizing how they interact and depend on each other.
- A clear explanation of the flow between the functions, detailing how data moves through the process from extraction to final output.
- A conclusion that ties together the entire process, emphasizing the role each function plays in achieving the final result.

Here you have a description of each function used, with the type of functions (extract, transform or insert),  the name of the function, the docstring of function, the input and output DataFrames or database tables for each one, and the code for the function:\n"""

    # Iterate over the functions to get only the information needed
    for func_id, func_info in info_dict.items():
        if func_info["type"] != process:
            continue

        input_key = "input"
        output_key = "output"
        if func_info["type"] == "extract":
            input_key = "db_table"
        elif func_info["type"] == "insert":
            output_key = "db_table"

        code = func_info["code"]
        if len(code.splitlines()) > 100:
            code = "***Code not included in prompt due to length.***"

        prompt += f"Type: {func_info["type"]}\n\n"
        prompt += f"Function: {func_info["name"]}\n\n"
        prompt += f"Docstring: {func_info["docstring"]}\n\n"
        prompt += f"Input DataFrames:\n{convert_df_to_string(func_info[input_key])}\n\n"
        prompt += (
            f"Output DataFrames:\n{convert_df_to_string(func_info[output_key])}\n\n"
        )
        prompt += f"Code:\n```\n{code}\n```\n\n"

    return prompt


def write_prompt(info_dict):
    title = "# Prompts for ETL Documentation\n\n"
    intro_title = "## Introduction\n\n"
    intro = write_intro_prompt(info_dict)
    extract_title = "## Extract\n\n"
    extract = write_process_prompt(info_dict, "extract")
    transform_title = "## Transform\n\n"
    transform = write_process_prompt(info_dict, "transform")
    insert_title = "## Insert\n\n"
    insert = write_process_prompt(info_dict, "insert")

    with open("doc_etl/prompt.md", "w") as f:
        f.write(title)
        f.write(intro_title)
        f.write(intro)
        f.write(extract_title)
        f.write(extract)
        f.write(transform_title)
        f.write(transform)
        f.write(insert_title)
        f.write(insert)


def find_dataframe_keys(data):
    """
    Find the keys in a dictionary that start with "DataFrame_"
    :param data: A dictionary
    :return: A set with the keys
    """
    unique_dataframes = set()

    # Recursive function to traverse the dictionary
    def extract_dataframes(d):
        if isinstance(d, dict):
            for key, value in d.items():
                if isinstance(key, str) and key.startswith("DataFrame_"):
                    # Add key to the set
                    unique_dataframes.add(key)
                if isinstance(value, (str, list, dict)):
                    # Recurse into substructures
                    extract_dataframes(value)
        elif isinstance(d, list):
            for item in d:
                extract_dataframes(item)

    extract_dataframes(data)
    return unique_dataframes


def apply_substitutions(original_dict, substitutions_dict):
    """
    Apply substitutions to the keys of a dictionary
    :param original_dict: A dictionary to make substitutions
    :param substitutions_dict: A dictionary with the substitutions
    :return: The dictionary with the substitutions
    """

    def recursive_replace(d):
        if isinstance(d, dict):
            return {
                substitutions_dict.get(k, k): recursive_replace(v) for k, v in d.items()
            }
        elif isinstance(d, list):
            return [recursive_replace(i) for i in d]
        else:
            return d

    return recursive_replace(original_dict)


def correct_df_names():
    """
    Correct the names of the DataFrames in the dictionary
    :return: A dictionary with the corrected names
    """
    path = "doc_etl/temp_mermaid.md"
    write_mermaid(INFO_DICT, path)

    unique_dataframes = find_dataframe_keys(INFO_DICT)

    substitutions = {}
    for dataframe in unique_dataframes:
        user_input = input(f"Enter substitution for {dataframe}: ")
        substitutions[dataframe] = user_input

    print("Substitutions:")
    for dataframe, substitution in substitutions.items():
        print(f"{dataframe} -> {substitution}")

    new_info_dict = apply_substitutions(INFO_DICT, substitutions)

    return new_info_dict


def write():
    """
    Write the documentation files
    :return: None
    """

    def create_folder_structure(folder_structure):
        """
        Create a folder structure if it does not exist
        :param List folder_structure: List of folders to create
        :return: None
        """
        _ = [os.makedirs(path, exist_ok=True) for path in folder_structure]

    folder_structure = ["doc_etl"]
    create_folder_structure(folder_structure)

    new_info_dict = correct_df_names()

    print(new_info_dict)

    write_json(new_info_dict)
    write_mermaid(new_info_dict)
    write_prompt(new_info_dict)

    print("Doc ETL process completed. Check './doc_etl' for details.")


# Register the write function to be executed at the end of the script
atexit.register(write)
