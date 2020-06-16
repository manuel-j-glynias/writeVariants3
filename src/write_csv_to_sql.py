import mysql.connector

from src.sql_utils import get_load_dir, get_schema, get_local_db_connection, drop_database, maybe_create_and_select_database, drop_table_if_exists, create_table, load_table


def write_csv_to_sql(descriptions_csv_path, should_drop_database):
    load_dir = get_load_dir()
    db_dict = get_schema(descriptions_csv_path)
    try:
        my_db = get_local_db_connection()
        my_cursor = my_db.cursor(buffered=True)
        if should_drop_database:
            db_name = list(db_dict.keys())[0]
            drop_database(my_cursor,db_name)

        for db_name in sorted(db_dict.keys()):
            maybe_create_and_select_database(my_cursor, db_name)
            for table_name in sorted(db_dict[db_name].keys()):
                drop_table_if_exists(my_cursor, table_name)
                create_table(my_cursor, table_name, db_name, db_dict)
                load_table(my_cursor, table_name, db_dict[db_name][table_name]['col_order'], load_dir)
        my_db.commit()
    except mysql.connector.Error as error:
        print("Failed in MySQL: {}".format(error))
    finally:
        if (my_db.is_connected()):
            my_cursor.close()


if __name__ == "__main__":
    descriptions_csv_path = '../config/table_descriptions_03_02.csv'
    should_drop_database = True
    write_csv_to_sql(descriptions_csv_path, should_drop_database)
