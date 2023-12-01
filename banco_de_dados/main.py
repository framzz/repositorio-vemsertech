from load import load_to_postgres, create_table_psql

if __name__ == "__main__":
    try:
        create_table_psql()
        load_to_postgres()
        print("Data loading completed.")
    except Exception as e:
        print(f'Failed to do the process. Error {e}')
