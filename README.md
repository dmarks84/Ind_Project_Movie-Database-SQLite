# Advanced SQL Movie Database

## Screenshot
![Example_TopDirectors](https://github.com/dmarks84/Ind_Project_Movie-Database-SQLite/blob/main/screenshot.png?raw=true)

## Summary
I utilzied two datasets on movies from Kaggle, one on Netflix's shows and the other on the top 1000 movies from IMDB.  I initially loaded these tables from CSV as they were received into a SQLite Database. I wrote several scripts, utilziing sqlite3 in python, to create new tables that better formatted the data (changing the datatypes) and creating primary keys.  I also create other tables to contain repetitive instances of films' directors, ratings, and genres.  The genre attribute represented a many-to-many relationship, so I created a linking table with to foreign keys.  Most actions for querying and inserting data into the tables was accomplished with custom functions I wrote and imported/called when needed.  The initial result in terms of the core data was a new, sleek table with id reference to related tables.

Using this core table, I created a number of queries and saved them as views related to meaningful questions.  The questions I investigated related to the most successful directors (again, this is limited to the most succesful directors whose movies made it into Netflix at the time the data was collected).  I developed queries utilizing GROUP BY, CTEs, WINDOW FUNCTIONS, and other aggregate functions to answer qusetions like, "What is the average gross at the box office for each director as a running average/total for each successive movie they made?" or "What is the average IMDB score for each director?"  The main answer I sought was to see which director, who had at least two films in the database, had the highest average gross at the box office for their films (A: Peter Jackson, as highlighted above in the screenshot of the SQLite database).

## Skills (Developed & Applied)
Programming, Python, SQL, SQLite, queries, commands, DDL, DML, DCL, DQL, Window Functions, Aggregate Functions, GROUP BY, CTEs
