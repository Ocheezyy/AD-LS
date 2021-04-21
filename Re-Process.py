import sqlite3 as sqlite
import os


db_path = "data/files.db"
conn = sqlite.connect(db_path)

def main():
    input_info = input("Please input the month and year to re-process (MM/YYYY)\n")
    input_info = input_info.strip(' ')
    input_date_split = input_info.split('/')
    select_query = f"SELECT filename FROM files WHERE filename LIKE '%{input_date_split[1]+input_date_split[0]}%'"
    # print(select_query)
    cursor = conn.cursor()
    cursor.execute(select_query)
    if cursor.rowcount > 0:
        print("There are no processed files for that date.")
        input("\nPress enter to close.....")
    else:
        delete_query = f"DELETE FROM files WHERE filename LIKE '%{input_date_split[1]+input_date_split[0]}%'"
        # print(delete_query)
        cursor.execute(delete_query)
        conn.commit()
        conn.close()
        print("File successfully re-processed")
        input("\nPress enter to close.....")


if __name__ == '__main__':
    main()