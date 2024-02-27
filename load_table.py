def load_table(conn, csvfile, table_name):
    """
    load_table loads a table into a SQLite database.  The 
    database must already exist and a connection established; 
    The data must be from a csv file and the table name specified.
    Messages are printed when errors occur.

    :param conn: connection to the SQLite database
    :param csvfile: string of file path/filename.csv
    :param table_name: string of the table name to load into
    :return: None
    """ 
    ### Import necessary methods
    from csv import reader
    from sql_funcs import create_table, insert_row
    from sql_funcs import table_create_comm, table_insert_comm

    try:
        ### Open the csvfile and record the first row as the column names
        try:
            with open(csvfile,'r',encoding='cp850') as f: 
                rows = reader(f)
                for idx, row in enumerate(rows):
                    if idx == 0:
                        table_cols = row
        except Exception as error:
            print('Issue with reading CSV file:',error)

        ### Create command template for creating the table in the database
        table_create = table_create_comm(table_name, table_cols)

        ### Create command template for inserting rows into database
        insert_command = table_insert_comm(table_name, table_cols)

        ### Create the table
        try:
            create_table(conn, table_create)
        except Exception as error:
            print('Issue with creating the table:',error)

        ### Read the csv file and insert each row into the database
        try:
            with open(csvfile,'r',encoding='cp850') as f: 
                rows = reader(f)
                for idx, row in enumerate(rows):
                    row_d = {key:val for key, val in list(zip(table_cols,row))}
                    if idx > 0:
                        insert_row(conn, insert_command, row_d)
                conn.commit()
        except Exception as error:
            print('Issue with reading CSV file:',error)
    except Exception as error:
        print('Unsuccessful in loading csvfile into the database')

### This will run during testing, when this script is called directly
if __name__ == '__main__':
    import sqlite3
    from sql_funcs import * 

    csvfile_net = 'data/netflix_titles.csv'
    csvfile_mov = 'data/imdb_top_1000.csv'
    ### Provide a name for the database and for the tables to write
    database = 'movies.db'
    table_net = 'netflix'
    table_imdb = 'imdb'
    ### Establish a connection to the database and attmept to create the table
    try:
        conn = sqlite3.connect(database)
    except:
        print(f'Could not connect to the database')
    
    ### If the tables already exist, drop them; in any case, load them up 
    ### load them up from the csv files
    drop_table(conn, table_net)
    drop_table(conn, table_imdb)
    load_table(conn, csvfile_net, table_net)
    load_table(conn, csvfile_mov, table_imdb)
    
    ### Close the connection to the database
    conn.close()
