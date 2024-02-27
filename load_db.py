### Import necessary packages and functions
import sqlite3
from load_table import load_table
from sql_funcs import *

### Define files for loading for base tables
csvfile_net = 'data/netflix_titles.csv'
csvfile_mov = 'data/imdb_top_1000.csv'
### Provide a name for the database and for the table to write into
database = 'movies.db'
table_net = 'netflix'
table_imdb = 'imdb'
table_joint = 'joint'
table_joint_temp = 'joint_temp'
table_director = 'director'
table_rating = 'rating'
table_genres = 'genres'
table_joint_genres = 'joint_genres'

### --------------------------------------------------
### Try to connect to the database
try:
    conn = sqlite3.connect(database)
except:
    print(f'Could not connect to the database to drop table')
### --------------------------------------------------
    
### --------------------------------------------------
### If the tables already exist, drop them; in any case, load them up 
### load them up from the csv files
drop_table(conn, table_net)
drop_table(conn, table_imdb)
drop_table(conn, table_joint)
load_table(conn, csvfile_net, table_net)
load_table(conn, csvfile_mov, table_imdb)
### --------------------------------------------------

### --------------------------------------------------
### Create a new, tempoary table with the movies that 
### show up in both the netflix and imdb tables

### Define the query on the base tables to get the data
### Most information is taken from the imdb table
joint_temp_query = f"""SELECT i.Series_Title AS Title,
                i.Released_Year AS Year_Released,
                i.Director,
                i.Runtime,
                i.Genre AS Genres,
                n.rating AS Rating,
                i.IMDB_Rating AS IMDB_Score,
                i.Meta_score AS Meta_Score,
                i.No_of_Votes AS Votes,
                i.Gross
            FROM {table_net} as n
            INNER JOIN {table_imdb} AS i
            ON n.title = i.Series_Title
            AND n.release_year = i.Released_Year
            WHERE n.type IS 'Movie';"""
### Define the columns that we'll be using for the temp joint table
joint_temp_cols = ['id','Title','Year_Released','Director','Runtime','Genres',
                   'Rating','IMDB_Score','Meta_Score','Votes','Gross']
### Define the datatypes for the table's columns
joint_temp_types = ['integer','text','integer','text','text','text',
                    'text','real','real','integer','integer']
### Get those values by querying the database
joint_temp_values = get_query(conn, joint_temp_query)
### Drop any exisitng tables by the same name
drop_table(conn, table_joint_temp)
### Create the new table (empty)
create_table(conn, 
             table_create_comm(table_joint_temp,joint_temp_cols,
                               types=joint_temp_types))
### Create a command string to combine iwth data dictionary to
### insert into the database
ins_comm = table_insert_comm(table_joint_temp, joint_temp_cols)
### Cycle through the values
for i, value in enumerate(joint_temp_values):
    ### Create an integer for the id
    value = [i+1, *value]
    ### Create a dictionary with the table columns and values
    value_d = {key:val for key, val in list(zip(joint_temp_cols,list(value)))}
    ### Insert the data for each query result as a new row in our new table
    insert_row(conn, ins_comm, value_d)
### Commit all of these insertions into the database
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### Create joint table similar to how we did before
### It'll be empty for now but establishes the type
### of data and connections we'll want to make  
joint_cols = ['id', 'Title','Year_Released','Director','Runtime',
              'Rating','IMDB_Score','Meta_Score','Votes','Gross']
joint_types = ['integer','text','integer','integer','integer',
               'integer','real','integer','integer','integer']
drop_table(conn, table_joint)
create_table(conn, 
             table_create_comm(table_joint,joint_cols,
                               types=joint_types))
### We'll fill the data in later
### --------------------------------------------------

### --------------------------------------------------
### Create a Directors table with each unique name
### and create a unique primary key id for each

### Find all the unique Directors and create a table
director_query = f"""SELECT DISTINCT Director
            FROM {table_joint_temp};"""
### Define columns for table and their types
director_cols = ['id','name']
director_types = ['integer','text']
### Get the naems of the directors
director_values = get_query(conn, director_query)
### Drop a Director table if it already exists
drop_table(conn, table_director)
### Create a new (empty) table
create_table(conn, 
             table_create_comm(table_director, director_cols, 
                               types=director_types, pk=['id'],
                               ### We'll add a foreign key
                               ### constraint between the director
                               ### and the director column in our
                               ### joint table
                               fks={'id':(table_joint,'id')}))
### Create string for inserting of Directors into the table,
### unique to each Director
ins_comm = table_insert_comm(table_director, director_cols)
for i, value in enumerate(director_values):
    value = [i+1, *value]
    value_d = {key:val for key, val in list(zip(director_cols,list(value)))}
    insert_row(conn, ins_comm, value_d)
### Commit the changes to the database
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### Create a Rating table with each unique rating
### and create a unique primary key id for each

### Steps followed are the same as those for the Director
### table noted above
rating_query = f"""SELECT DISTINCT Rating
            FROM {table_joint_temp};"""
rating_cols = ['id','rating']
rating_types = ['integer','text']
rating_values = get_query(conn, rating_query)
drop_table(conn, table_rating)
create_table(conn, table_create_comm(table_rating, rating_cols, 
                                     types=rating_types, pk=['id'],
                                     fks={'id':(table_joint,'id')}))
ins_comm = table_insert_comm(table_rating, rating_cols)
for i, value in enumerate(rating_values):
    value = [i+1, *value]
    value_d = {key:val for key, val in list(zip(rating_cols,list(value)))}
    insert_row(conn, ins_comm, value_d)
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### Create a Genres table with each unique rating
### and create a unique primary key id for each
genres_query = f"""SELECT Genres
            FROM {table_joint_temp};"""
genres_cols = ['id','Genre']
genres_types = ['integer','text']
genres_values = get_query(conn, genres_query)
### Since Genres are provided in a list, we need to find
### the unique individual genres from all lists
genres_set = set()
for value in genres_values:
    genres_set = set([*genres_set,*value[0].split(', ')])
### Now drop any old tables and create a new one
drop_table(conn, table_genres)
### No foreign key needed here as we'll use a linking table (next section)
create_table(conn, table_create_comm(table_genres, genres_cols, 
                                     types=genres_types, pk=['id']))
ins_comm = table_insert_comm(table_genres, genres_cols)
for i, value in enumerate(genres_set):
    value = [i+1, value]
    value_d = {key:val for key, val in list(zip(genres_cols,list(value)))}
    insert_row(conn, ins_comm, value_d)
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### Create a table to link a many-to-many relationship
### between movies and genres, since movies can have
### multiple genres and genres can apply to many 
### different movies

### Define columns and their types for the linked table
table_joint_genres_cols = ['id','joint_id','genre_id']
joint_genres_types = ['integer','integer','integer']
### Drop existing table of the same name and creat a new
### empty one.
drop_table(conn, table_joint_genres)
create_table(conn, 
             table_create_comm(
                 table_joint_genres, table_joint_genres_cols,
                 types=joint_genres_types, 
                 pk=['id'], 
                 ### We'll use a set of foreign keys to connect
                 ### to the joint movie table and the genre table
                 fks={'joint_id':(table_joint,'id'),
                      'genre_id':(table_genres,'id')}))
### Get the values of genres and the associated id from the 
### (temp) joint table
query = f"""SELECT id, Genres
            FROM {table_joint_temp};"""
result = get_query(conn, query)
### Establish a primary key value for our linking table
idx = 1
### We'll loop through each genre and id pair...
for r in result:
    ### ...assign the id for the joint table as the id of the result...
    j_id = r[0]
    ### ...then for each genre from the return list of genres for a movie...
    for g in r[1].split(', '):
        ### ...get the associated id from the genres table
        q = f"""SELECT id FROM {table_genres} WHERE Genre = '{g}';"""
        g_id = get_query(conn, q)[0][0]
        ### Format data for insertion
        d = {'id':idx, 'joint_id':j_id, 'genre_id':g_id}
        ### ...write this into our linking table
        ins_comm = table_insert_comm(table_joint_genres, table_joint_genres_cols)
        insert_row(conn, ins_comm, d)
        ### Move to next primary key value and repeat
        idx += 1
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### We'll do the same thing for our joint table as we
### did for our temporary table.  This time, however,
### we can insert all of the values we have ready to 
### go from our previous tables
joint_query = f"""SELECT i.Series_Title AS Title,
                i.Released_Year AS Year_Released,
                i.Director,
                i.Runtime,
                n.rating AS Rating,
                i.IMDB_Rating AS IMDB_Score,
                i.Meta_score AS Meta_Score,
                i.No_of_Votes AS Votes,
                i.Gross
            FROM {table_net} as n
            INNER JOIN {table_imdb} AS i
            ON n.title = i.Series_Title
            AND n.release_year = i.Released_Year
            WHERE n.type IS 'Movie';"""
joint_values = get_query(conn, joint_query)
ins_comm = table_insert_comm(table_joint, joint_cols)
for i, value in enumerate(joint_values):
    title = str(value[0])
    year = int(value[1])
    director = str(value[2])
    dir_q = f"""SELECT id FROM {table_director} WHERE name = '{director}';"""
    d_id = get_query(conn, dir_q)[0][0]
    run = int(value[3].split(' ')[0])
    rating = str(value[4])
    rat_q = f"""SELECT id FROM {table_rating} WHERE rating = '{rating}';"""
    r_id = get_query(conn, rat_q)[0][0]
    i_score = float(value[5])
    try:
        m_score = int(value[6])
    except:
        m_score = None
    votes = int(value[7])
    try:
        gross = int(''.join(value[8].split(',')))
    except:
        gross = None
    vals = [title,year,d_id,run,r_id,i_score,m_score,votes,gross]
    vals = [i+1, *vals]
    value_d = {key:val for key, val in list(zip(joint_cols,list(vals)))}
    insert_row(conn, ins_comm, value_d)
conn.commit()
### --------------------------------------------------

### --------------------------------------------------
### Quick query to potentially check the PRAGMA of a given table
# query = f"PRAGMA table_info({table_joint});"
# print(get_query(conn, query))
### --------------------------------------------------

### Close the connection to the database
conn.close()