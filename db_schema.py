from const import data_schema


def parse_schema(schema):
    columns = []
    for column in schema:
        column_name = column[2]
        column_type = column[3]
        columns.append({"column_name": column_name, "column_type": column_type})
    return columns


def get_tables(schema, table):
    database_dict = {}
    for row in schema:
        table_name = row[1]
        if table_name != table:
            continue
        if table_name not in database_dict:
            database_dict[table_name] = []
        database_dict[table_name].append(row)

    for table_name, table_schema in database_dict.items():
        table = {
            table_name: parse_schema(table_schema)
        }
    return table


if __name__ == "__main__":
    table = "BICI_DM_EVO_CANAL_VENTA"
    table_schema = get_tables(data_schema, table)

    print(table_schema)




