import sqlite3

def table_create_comm(table_name, table_cols, types=[], pk=[], fks={}):
    """table_create_comm creates a string of a SQLite command that 
    will create a table given inputted parameters.

    :param table_name: string of the table name
    :param table_cols: list of columns
    :param types(opt): list of string datatypes for table columns
    :param pk(opt): list containing single string primary key column
    :param fks(opt): dictionary of foreign keys; key is table column, values
        are the foreign table and its related column
    :return: formatted string, ready for use for SQLite command
    """ 
    if types:
        types_d = {key:val for key, val in list(zip(table_cols,list(types)))}
    comm = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    for idx, col in enumerate(table_cols):
        if types:
            comm += f'{col} {types_d[col]}'
        else:
            comm += f'{col} text'
        if (not pk) and (idx == 0):
            comm += ' PRIMARY KEY'
        if pk and (col == pk[0]):
            comm += ' PRIMARY KEY'
        if idx < len(table_cols)-1:
            comm += ', '
    for idx, col in enumerate(list(fks.keys())):
        fk_table = fks[col][0]
        fk_id = fks[col][1]
        comm += f', FOREIGN KEY({col}) REFERENCES {fk_table}({fk_id})'
    return comm + ');'

def table_insert_comm(table_name, table_cols):
    """table_insert_comm creates a string of a SQLite command that 
    can be used to insert inputted parameters and later-defined values
    into a give table.

    :param table_name: string of the table name
    :param table_cols: list of columns
    :return: formatted string, ready for use for SQLite command
    """ 
    comm = f'INSERT INTO {table_name} VALUES('
    for idx, col in enumerate(table_cols):
        comm += f':{col}'
        if idx < len(table_cols)-1:
            comm += ', '
    return comm + ');'

def drop_table(conn, table_name):
    """drop_table drops a table from a given connected databsae

    :param conn: exisitng database connection object
    :param table_name: string of the table name
    :return: None.  Errors printed as they arise.
    """ 
    try:
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS {table_name};')
        conn.commit()
        cur.close()
    except Exception as error:
        print('Unable to drop table:',error)

def drop_view(conn, view_name):
    """drop_view drops a view from a given connected databsae

    :param conn: exisitng database connection object
    :param table_name: string of the table name
    :return: None.  Errors printed as they arise.
    """ 
    try:
        cur = conn.cursor()
        cur.execute(f'DROP VIEW IF EXISTS {view_name};')
        conn.commit()
        cur.close()
    except Exception as error:
        print('Unable to drop view:',error)

def create_table(conn, command):
    """create_table creates a table from a given connected databsae

    :param conn: exisitng database connection object
    :param command: string of command to create the table, likely
    provided by table_create_comm
    :return: None.  Errors printed as they arise.
    """ 
    try:
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
    except Exception as error:
        print('Unable to create table:',error)

def create_view(conn, command):
    """create_view creates a view from a given connected databsae

    :param conn: exisitng database connection object
    :param command: string of command to view the table
    :return: None.  Errors printed as they arise.
    """ 
    try:
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
    except Exception as error:
        print('Unable to create view:',error)

def get_query(conn, query):
    """get_query gets the results of a query from a connected databsae
    using a defined query string

    :param conn: exisitng database connection object
    :param query: string of query
    :return: result of query to database's table (defined in query)
    """ 
    try:
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        return result
    except Exception as error:
        print('Unable to execute query:',error)

def insert_row(conn, command, data):
    """insert_row inserts data into a table in a connected 
    database.

    :param conn: exisitng database connection object
    :param command: string of command, likely provided by table_insert_comm
    :param data: dictionary of data; keys are table columns, values are data
    to be inserted
    :return: None.  Errors printed as they arise.
    """ 
    try:
        cur = conn.cursor()
        cur.execute(command, data)
        cur.close()
    except sqlite3.Error as er:
        print('SQLite error: %s' % (' '.join(er.args)))
        print("Exception class is: ", er.__class__)
        print('SQLite traceback: ')
    except Exception as error:
        print('Unable to add alert:',error)