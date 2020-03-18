import psycopg2
import os
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values
import sqlite3

load_dotenv() #look in the .env file for env vars, and add them to the env

DB_NAME = os.getenv("DB_NAME", default="OOPS")
DB_USER = os.getenv("DB_USER", default="OOPS")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS")
DB_HOST = os.getenv("DB_HOST", default="OOPS")

### Connect to ElephantSQL-hosted PostgreSQL
connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASSWORD, host=DB_HOST)
#print("CONNECTION:", connection)

### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor()
#print('CURSOR:', cursor)

##Create a table

#Get rpg data
conn = sqlite3.connect('rpg_db.sqlite3')
data = conn.execute('SELECT * FROM charactercreator_character;').fetchall()
#cursor = conn.cursor()

#Test to see if db is loaded
#query = 'SELECT COUNT(*) FROM charactercreator_character;'
#print(f"There are a total of {cursor.execute(query).fetchall()[0][0]} characters")

char_table = """ 
CREATE TABLE charactercreator_character (
  character_id SERIAL PRIMARY KEY,
  name varchar(30),
  level int,
  exp int,
  hp int, 
  strength int,
  intelligence int,
  dexterity int,
  wisdom int
);
"""

cursor.execute(char_table)

for i in data:
  insert_data = """INSERT INTO charactercreator_character
        (name, level, exp, hp, strength, intelligence, dexterity, wisdom)
        VALUES""" + str(i[1:])
  cursor.execute(insert_data)
connection.commit()
    

#Still trying to figure this out

#insertion_query = "INSERT INTO char_table (name, data) VALUES %s"
#execute_values(data, insertion_query, conn) 




