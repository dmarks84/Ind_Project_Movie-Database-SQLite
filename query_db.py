import sqlite3
from load_table import load_table
from sql_funcs import *

### Provide a name for the database and for the table to write into
database = 'movies.db'
table_net = 'netflix'
table_imdb = 'imdb'

### Try to connect to the database
try:
    conn = sqlite3.connect(database)
except:
    print(f'Could not connect to the database to drop table')

### --------------------------------------------------
### Sample Queries -- Pull out some info from each table
# query1 = f"""SELECT show_id  FROM {table_name1} WHERE rating in ('R','PG-13')"""
# result1 = get_query(conn, query1)
# query2 = f"""SELECT Series_Title  FROM {table_name2} WHERE Director like 'Quent%'"""
# result2 = get_query(conn, query2)
# print("Query 1 Results:",len(result1))
# print("Query 2 Results:",len(result2))
### --------------------------------------------------

### --------------------------------------------------
### Sample Queries -- Select movies from movie tables and joined tables
### Find the title of the movies in the netflix table
q_netmovies = f"""SELECT title          
            FROM {table_net}
            WHERE type IS 'Movie';"""
### Use list comprehension to just provide the titles
r_netmovies = [movie[0] for movie in get_query(conn, q_netmovies)]

### Find the count of the movies in the netflix table
q_netmovies_ct = f"""SELECT COUNT(*)           
            FROM {table_net}
            WHERE type IS 'Movie';"""
r_netmovies_ct = get_query(conn, q_netmovies_ct)[0][0]
### Verify these previous two queries are producing expected results (i.e., same count)
assert len(r_netmovies) == r_netmovies_ct, \
    "Possible error in number of movies in netflix database"

### Find the title of the movies in the imdb table
q_imdbmovies = f"""SELECT Series_Title          
            FROM {table_imdb};"""
### Use list comprehension to just provide the titles
r_imdbmovies = [movie[0] for movie in get_query(conn, q_imdbmovies)]

### Find the count of the movies in the imdb table
q_imdbmovies_ct = f"""SELECT COUNT(*)           
            FROM {table_imdb};"""
r_imdbmovies_ct = get_query(conn, q_imdbmovies_ct)[0][0]
### Verify these previous two queries are producing expected results (i.e., same count)
assert len(r_imdbmovies) == r_imdbmovies_ct, \
    "Possible error in number of movies in netflix database"

### Find the movies that show up in both tables
q_both = f"""SELECT n.title 
            FROM {table_net} AS n
            INNER JOIN {table_imdb} AS i
            ON n.title = i.Series_Title
            AND n.release_year = i.Released_Year
            WHERE n.type IS 'Movie';"""
### Use list comprehension to just provide the titles
r_both = [title[0] for title in get_query(conn, q_both)]

### Find the movies that only show up in the netflix table
q_nonly = f"""SELECT n.title
            FROM 
                (SELECT * FROM {table_net} WHERE type IS 'Movie') AS n
            LEFT OUTER JOIN {table_imdb} AS i
            ON n.title = i.Series_Title
            AND n.release_year = i.Released_Year
            WHERE i.Series_Title IS NULL;"""
### Use list comprehension to just provide the titles
r_nonly = [title[0] for title in get_query(conn, q_nonly)]

### Find the movies that only show up in the imdb table
q_ionly = f"""SELECT i.Series_Title
            FROM {table_imdb} AS i
            LEFT OUTER JOIN 
                (SELECT * FROM {table_net} WHERE type IS 'Movie') AS n
            ON i.Series_Title = n.title
            AND n.release_year = i.Released_Year
            WHERE n.title IS NULL;"""
### Use list comprehension to just provide the titles
r_ionly = [title[0] for title in get_query(conn, q_ionly)]

# print(f'Number of movies in {table_net} table:',r_netmovies_ct)
# print(f'Number of movies in {table_imdb} table:',r_imdbmovies_ct)
# print(f'Number of movies found in {table_net} AND {table_imdb} tables:', len(r_both))
# print(f'Number of movies only found in {table_net} table:',len(r_nonly))
# print(f'Number of movies only found in {table_imdb} table:',len(r_ionly))
### --------------------------------------------------

### --------------------------------------------------
q_both = f"""SELECT n.*, i.*
            FROM {table_net} AS n
            INNER JOIN {table_imdb} AS i
            ON n.title = i.Series_Title
            AND n.release_year = i.Released_Year
            WHERE n.type IS 'Movie';"""
r_both = get_query(conn, q_both)
print(len(r_both))

conn.close()