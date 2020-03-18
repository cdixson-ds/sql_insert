import pandas as pd
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
print("CONNECTION:", connection)

### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor()
print('CURSOR:', cursor)



df = pd.read_csv('titanic.csv')
conn = sqlite3.connect("titanic.sqlite3")

df.to_sql("titanic.sqlite3", conn, index_label='id')



#Create Table

create_table = """ 
CREATE TABLE titanic_table (
  Survived int, 
  Pclass int,
  Name varchar(100),
  Sex varchar(20),
  Age float,
  SiblingsSpouses int,
  ParentsChildren int,
  Fare float
);
"""

data = conn.execute('SELECT * FROM "titanic.sqlite3"').fetchall()

cursor.execute(create_table)

for i in data:
  insert_data = """INSERT INTO titanic_table
        (Survived, Pclass, Name, Sex, Age, SiblingsSpouses, ParentsChildren, Fare)
        VALUES""" + str(i[1:])
  cursor.execute(insert_data)
connection.commit()
