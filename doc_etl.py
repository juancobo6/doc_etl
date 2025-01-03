import os
import functools
import inspect
import json

import atexit
import pandas as pd
import polars as pl

# TODO: Find a better way to handle getting the table schema
from const import data_schema as DB_SCHEMA


INFO_DICT = {}


def log_dataframe_info(df):
    """Log details of a DataFrame (compatible with Pandas and Polars)."""
    label = f"DataFrame_{id(df)}"  # Use the unique id of the DataFrame as the label

    if isinstance(df, pd.DataFrame):
        return {
            label: df.columns.tolist()
        }
    elif isinstance(df, pl.DataFrame):
        return {
            label: df.columns  # Polars columns property
        }
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df).__name__}")


def parse_schema(schema):
    columns = []
    for column in schema:
        column_name = column[2]
        columns.append(column_name)
    return columns


def get_table(table):
    database_dict = {
        table: []
    }
    for row in DB_SCHEMA:
        table_name = row[1]
        if table_name != table:
            continue
        database_dict[table_name].append(row)

    for table_name, table_schema in database_dict.items():
        table = {
            table_name: parse_schema(table_schema)
        }
    return table


def extract(extract_table):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)

            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "extract"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")

            # TODO: Find a better way to handle getting the table schema
            INFO_DICT[function_id]["db_table"] = []
            if isinstance(extract_table, (list, tuple)):
                for table in extract_table:
                    INFO_DICT[function_id]["db_table"].append(get_table(table))
            elif isinstance(extract_table, str):
                INFO_DICT[function_id]["db_table"].append(get_table(extract_table))

            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            if isinstance(result, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["output"].append(
                    log_dataframe_info(result)
                )
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
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "transform"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")
            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["db_table"] = []
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            for i, arg in enumerate(args):
                if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["input"].append(
                        log_dataframe_info(arg)
                    )

            result = func(*args, **kwargs)

            if isinstance(result, (pd.DataFrame, pl.DataFrame)):
                INFO_DICT[function_id]["output"].append(
                    log_dataframe_info(result)
                )
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
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args_id = [id(arg) for arg in args]
            args_id = "_".join(map(str, args_id))
            function_id = str(id(func)) + "_" + args_id
            INFO_DICT[function_id] = {}

            INFO_DICT[function_id]["name"] = func.__name__
            INFO_DICT[function_id]["type"] = "insert"
            INFO_DICT[function_id]["docstring"] = func.__doc__.replace("\n", " ")

            # TODO: Find a better way to handle getting the table schema
            INFO_DICT[function_id]["db_table"] = []
            if isinstance(insert_table, (list, tuple)):
                for table in insert_table:
                    INFO_DICT[function_id]["db_table"].append(get_table(table))
            elif isinstance(insert_table, str):
                INFO_DICT[function_id]["db_table"].append(get_table(insert_table))

            INFO_DICT[function_id]["code"] = inspect.getsource(func).strip()
            INFO_DICT[function_id]["input"] = []
            INFO_DICT[function_id]["output"] = []

            for i, arg in enumerate(args):
                if isinstance(arg, (pd.DataFrame, pl.DataFrame)):
                    INFO_DICT[function_id]["input"].append(
                        log_dataframe_info(arg)
                    )

            result = func(*args, **kwargs)

            return result
        return wrapper
    return decorator


def write_json(info_dict):
    with open("doc_etl/raw.json", "w") as f:
        json.dump(info_dict, f, indent=4)


def write_mermaid(info_dict, path="doc_etl/mermaid.md"):
    string = """
```mermaid
graph TD

    """

    for func_id, func_info in info_dict.items():
        tables = func_info.get("db_table", [])
        for table in tables:
            for table_name, columns in table.items():
                columns_string = "\n" + "\n".join([f"{column}" for column in columns])
                label_columns = "_" + str(table_name) +  "_" + str(columns_string)
                label_string = str(table_name) + f'[("`{label_columns}`")]'

                if func_info["type"] == "extract":
                    string += f"{label_string} --> {func_id}[/{func_info['name']}/]\n"
                elif func_info["type"] == "insert":
                    string += f"{func_id}[/{func_info['name']}/] --> {label_string}\n"

        for input_df in func_info["input"]:
            for label, info in input_df.items():
                if len(info) > 4:
                    columns_string = "\n".join(info[:3]) + "\n(...)\n " + info[-1]
                else:
                    columns_string = "\n" + "\n ".join(info)

                label_columns = "_" + str(label) + "_" + columns_string

                label_string = str(label) + '@' + '{ shape: braces, label: "' + label_columns + '" }'
                string += f"{label_string} --> {func_id}[/{func_info["name"]}/]\n"
        for output_df in func_info["output"]:
            for label, info in output_df.items():
                if len(info) > 4:
                    columns_string = "\n".join(info[:3]) + "\n(...)\n " + info[-1]
                else:
                    columns_string = "\n" + "\n ".join(info)
                label_columns = "_" + str(label) + "_" + columns_string

                label_string = str(label) + '@' + '{ shape: braces, label: "' + label_columns + '" }'
                string += f"{func_id}[/{func_info["name"]}/] --> {label_string}\n"

    string += "```"

    with open(path, "w") as f:
        f.write(string)


def convert_df_to_string(dfs):
    return_string = ""
    for dict_df in dfs:
        for table_name, columns in dict_df.items():
            columns_string = "\n - " + "\n - ".join([f"{column}" for column in columns]) + "\n"
            return_string += f"{table_name}: {columns_string}\n"

    return return_string


def write_intro_prompt(info_dict):
    prompt = """Please redact the introduction to the documentation for this ETL process, focusing on the different relationships between the various executions of the functions. This section should only be an introduction, as there will be specific sections made for extraction, transformations and insertions. The response should be structured with clear sections and titles, with an overall overview of the whole ETL process. The overall tone should be narrative, with long sentences that explain the flow and interconnectedness of the functions. Ensure that the output reflects the complexity of the process while being organized and easy to follow. The documentation should include:

- An overview of the ETL process, summarizing its purpose and main stages.
- A clear explanation of the flow between the functions, detailing how data moves through the process from extraction to final output.
- A conclusion that ties together the entire process, emphasizing the role each function plays in achieving the final result.

Here you have a brief description of each function used, with the type of functions (extract, transform or insert),  the name of the function, the docstring of function, and the input and output DataFrames or database tables for each one:\n\n"""

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
        prompt += f"Output DataFrames:\n{convert_df_to_string(func_info[output_key])}\n\n"

    return prompt


def write_process_prompt(info_dict, process):
    prompt = f"""Please redact the documentation for the {process} part in this ETL process, focusing on the different relationships between the various executions of the functions. The response should be structured with clear sections and titles, each addressing specific aspects of the {process} process. The overall tone should be narrative, with long sentences that explain the flow and interconnectedness of the functions. Ensure that the output reflects the complexity of the process while being organized and easy to follow. The documentation should include:

- A detailed description of each function in the {process} process, emphasizing how they interact and depend on each other.
- A clear explanation of the flow between the functions, detailing how data moves through the process from extraction to final output.
- A conclusion that ties together the entire process, emphasizing the role each function plays in achieving the final result.

Here you have a description of each function used, with the type of functions (extract, transform or insert),  the name of the function, the docstring of function, the input and output DataFrames or database tables for each one, and the code for the function:\n"""

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
        prompt += f"Output DataFrames:\n{convert_df_to_string(func_info[output_key])}\n\n"
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
    unique_dataframes = set()

    # Recursive function to traverse the dictionary
    def extract_dataframes(d):
        if isinstance(d, dict):
            for key, value in d.items():
                if isinstance(key, str) and key.startswith("DataFrame_"):
                    unique_dataframes.add(key)  # Add key to the set
                if isinstance(value, (str, list, dict)):
                    extract_dataframes(value)  # Recurse into substructures
        elif isinstance(d, list):
            for item in d:
                extract_dataframes(item)

    extract_dataframes(data)
    return unique_dataframes


def apply_substitutions(original_dict, substitutions_dict):
    def recursive_replace(d):
        if isinstance(d, dict):
            return {substitutions_dict.get(k, k): recursive_replace(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [recursive_replace(i) for i in d]
        else:
            return d

    return recursive_replace(original_dict)

def correct_df_names():
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


atexit.register(write)


