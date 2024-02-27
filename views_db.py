### Import necessary packages and functions
import sqlite3
from sql_funcs import *

database = 'movies.db'
table_movies = 'joint'
table_dir = 'director'
table_rat = 'rating'
table_link = 'joint_genres'
table_gen = 'genres'
view_core = 'Netflix_Movies'
view_dirs = 'Top_Directors'
view_gross = 'Gross_by_Dir'
view_bestdir = 'Best_Director'

### ----------------------------------------------------
### Connect to the database
try:
    conn = sqlite3.connect(database)
except:
    print(f'Could not connect to the database to drop table')
### ----------------------------------------------------

### ----------------------------------------------------
### This query will pull out all of the relevant information
### from the table we added to the database, which was the
### union between the netflix and imdb base tables.  This 
### query also pulls out the information from the tables
### created for the names of the directors, genres, and
### ratings associated with each movie.
full_query = f"""SELECT mov.Title AS 'Title',
            mov.Year_Released AS 'Year',
            dir.name AS 'Director',
            rat.rating AS 'Rating',
            GROUP_CONCAT(gen.Genre,', ') AS 'Genres',
            mov.Runtime AS 'Run Time',
            mov.IMDB_Score AS 'IMDB Score',
            mov.Meta_Score AS 'Meta Score',
            mov.Votes AS 'Votes',
            mov.Gross AS 'Box Office (Gross)'
            FROM {table_movies} AS mov
            LEFT OUTER JOIN {table_dir} AS dir
            ON mov.Director = dir.id
            LEFT OUTER JOIN {table_rat} AS rat
            ON mov.Rating = rat.id
            INNER JOIN {table_link} AS link
            ON mov.id = link.joint_id
            JOIN {table_gen} AS gen
            ON gen.id = link.genre_id
            GROUP BY mov.Title
            ORDER BY mov.Title;"""
### Drop and create/save a view based on the query
drop_view(conn, view_core)
command = f'CREATE VIEW {view_core} AS ' + full_query
create_view(conn, command)
# query = f'SELECT * FROM {view_core};'
# print(get_query(conn, query))
### ----------------------------------------------------

### ----------------------------------------------------
### This query pulls out the top directors, their movie
### count, the names of their movies, the average runtime
### of their movies, and the average "scores" of their
### movies in terms of website score, votes, and average/
### total gross at the box office.  It uses GROUP BY 
### to look at these values per director.
topdir_query = f"""SELECT dir.name AS 'Director',
            COUNT(mov.Title) AS 'Total Movies',
            GROUP_CONCAT(mov.Title,', ') AS 'Movies',
            AVG(mov.Runtime) AS 'Average Run Time',
            AVG(mov.IMDB_Score) AS 'Average IMDB Score',
            AVG(mov.Meta_Score) AS 'Average Meta Score',
            AVG(mov.Votes) AS 'Average Votes',
            AVG(mov.Gross) AS 'Average Film Gross',
            SUM(mov.Gross) AS 'Total Films Gross'
            FROM {table_movies} AS mov
            LEFT OUTER JOIN {table_dir} AS dir
            ON mov.Director = dir.id
            GROUP BY mov.Director
            ORDER BY AVG(mov.Gross) DESC"""
### Drop and create/save a view based on the query
drop_view(conn, view_dirs)
command = f'CREATE VIEW {view_dirs} AS ' + topdir_query+';'
create_view(conn, command)
# query = f'SELECT * FROM {view_dirs};'
# print(get_query(conn, query))
### ----------------------------------------------------

### ----------------------------------------------------
### This query uses window functions to look at each film
### in the database and partition them by director, creating
### running totals for the count of movies they've done.
### We also track a running total of gross at the box office
### As well as their updated average they earned at the box
### per film. Entries (movies) are ordered by release year.
gross_query = f"""SELECT mov.Title as 'Film',
            mov.Year_Released as 'Year',
            dir.name AS 'Director',
            mov.Gross AS 'Film Gross',
            COUNT(mov.Title) OVER
                (PARTITION BY dir.name ORDER BY mov.Year_Released) 
                AS 'No. Films',
            AVG(mov.Gross) OVER
                (PARTITION BY dir.name ORDER BY mov.Year_Released) 
                AS 'Average Gross',
            SUM(mov.Gross) OVER
                (PARTITION BY dir.name ORDER BY mov.Year_Released) 
                AS 'Total Films Gross'
            FROM {table_movies} AS mov
            LEFT OUTER JOIN {table_dir} AS dir
            ON mov.Director = dir.id
            ORDER BY mov.Year_Released;"""
### Drop and create/save a view based on the query
drop_view(conn, view_gross)
command = f'CREATE VIEW {view_gross} AS ' + gross_query
create_view(conn, command)
# query = f'SELECT * FROM {view_gross};'
# print(get_query(conn, query))
### ----------------------------------------------------

### ----------------------------------------------------
### THis query takes the previous "top directors" view
### and creates a CTE from this, then selecting the 
### Director with the highest average box office pull
### per movie for directors with more than 1 movie in 
### the database.
bestdir_query = f"""WITH topdirs AS ({topdir_query})
                    SELECT topdirs.Director
                    FROM topdirs
                    WHERE topdirs.'Total Movies' > 1
                    LIMIT 1;"""
### Drop and create/save a view based on the query
drop_view(conn, view_bestdir)
command = f'CREATE VIEW {view_bestdir} AS ' + bestdir_query
create_view(conn, command)
# query = f'SELECT * FROM {view_bestdir};'
# print(get_query(conn, query)[0][0])
### ----------------------------------------------------

conn.close()