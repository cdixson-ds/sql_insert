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
#print("CONNECTION:", connection)                                  #check connection

### A "cursor", a structure to iterate over db records to perform queries
cursor = connection.cursor()
#print('CURSOR:', cursor)                                           ###


df = pd.read_csv('titanic.csv')

#conn = sqlite3.connect("titanic.sqlite3")
#df.to_sql("titanic.sqlite3", conn, index_label='id')


#Create Table

create_table = """ 
CREATE TABLE titanic_table (
  id SERIAL PRIMARY KEY,
  Survived int, 
  Pclass int,
  Name varchar(100),
  Sex varchar(20),
  Age int,
  Sib_Spouses_Count int,
  Parent_Child_Count int,
  Fare float
);
"""

#data = conn.execute('SELECT * FROM "titanic.sqlite3"').fetchall()

#cursor.execute(create_table)                                                          #######uncomment later                

#for i in data:
#  insert_data = """INSERT INTO titanic_table
#        (Survived, Pclass, Name, Sex, Age, Sib_Spouses_Count, Parent_Child_Count, Fare)
#        VALUES""" + str(i[1:])
#  cursor.execute(insert_data)

#if len(data) == 0:
    # INSERT RECORDS
    #CSV_FILEPATH = "data/titanic.csv"

#CSV_FILEPATH = os.path.join(os.path.dirname(__file__), "..", "data", "titanic.csv")
#print("FILE EXISTS?", os.path.isfile(CSV_FILEPATH))
#df = pd.read_csv(CSV_FILEPATH)
#print(df.head())
    # rows should be a list of tuples
    # [
    #   ('A rowwwww', 'null'),
    #   ('Another row, with JSONNNNN', json.dumps(my_dict)),
    #   ('Third row', "3")
    # ]
    # h/t Jesus and https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.itertuples.html


#rows = list(df.itertuples(index=False, name=None))
#insertion_query = "INSERT INTO titanic_table (Survived, Pclass, Name, Sex, Age, Sib_Spouses_Count, Parent_Child_Count, Fare) VALUES %s"   #####uncomment later
#execute_values(cursor, insertion_query, rows)

#connection.commit()

print('How many passengers survived, and how many died?')

query = """
SELECT COUNT(titanic_table)
FROM titanic_table
WHERE titanic_table.survived > 0
"""
cursor.execute(query)
rows = cursor.fetchone()
print(rows[0], 'passengers survived')

query2 = """
SELECT COUNT(titanic_table)
FROM titanic_table
WHERE titanic_table.survived < 1
"""
cursor.execute(query2)
rows = cursor.fetchone()
print(rows[0], 'passengers died')
print('_______________________________________')


print('How many passengers were in each class?')

query3 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 3
"""
cursor.execute(query3)
rows = cursor.fetchone()
print(rows[0], 'passengers in class 3')

query4 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 2
"""
cursor.execute(query4)
rows = cursor.fetchone()
print(rows[0], 'passengers in class 2')

query5 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 1
"""
cursor.execute(query5)
rows = cursor.fetchone()
print(rows[0], 'passengers in class 1')

print('_______________________________________')

print('How many passengers survived/died within class 3?')

query6 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 3 AND survived > 0
"""
cursor.execute(query6)
rows = cursor.fetchone()
print(rows[0], 'class 3 passengers survived')

query7 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 3 AND survived < 1
"""
cursor.execute(query7)
rows = cursor.fetchone()
print(rows[0], 'class 3 passengers died')

print('_______________________________________')

print('How many passengers survived/died within class 2?')

query8 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 2 AND survived > 0
"""
cursor.execute(query8)
rows = cursor.fetchone()
print(rows[0], 'class 2 passengers survived')

query9 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 2 AND survived < 1
"""
cursor.execute(query9)
rows = cursor.fetchone()
print(rows[0], 'class 2 passengers died')

print('_______________________________________')

print('How many passengers survived/died within class 1?')

query8 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 1 AND survived > 0
"""
cursor.execute(query8)
rows = cursor.fetchone()
print(rows[0], 'class 1 passengers survived')

query9 = """
SELECT COUNT(pclass) FROM titanic_table
WHERE titanic_table.pclass = 1 AND survived < 1
"""
cursor.execute(query9)
rows = cursor.fetchone()
print(rows[0], 'class 1 passengers died')

print('_______________________________________')

print('What was the average age of survivors vs nonsurvivors?')

query10 = """
SELECT AVG(age) FROM titanic_table
WHERE survived > 0
"""
cursor.execute(query10)
rows = cursor.fetchone()
print(rows[0], 'is the avearge age of survivors')

query11 = """
SELECT AVG(age) FROM titanic_table
WHERE survived < 1
"""
cursor.execute(query11)
rows = cursor.fetchone()
print(rows[0], 'is the average age of nonsurvivors')

print('_______________________________________')

print('What was the average age for class 3 passengers?')

query12 = """
SELECT AVG(age) FROM titanic_table
WHERE pclass = 3
"""
cursor.execute(query12)
rows = cursor.fetchone()
print(rows[0], 'is the average age for class 3 passengers')

print('_______________________________________')

print('What was the average age for class 2 passengers?')

query13 = """
SELECT AVG(age) FROM titanic_table
WHERE pclass = 2
"""
cursor.execute(query13)
rows = cursor.fetchone()
print(rows[0], 'is the average age for class 2 passengers.')

print('_______________________________________')

print('What was the average age for class 1 passengers?')

query14 = """
SELECT AVG(age) FROM titanic_table
WHERE pclass = 1
"""

cursor.execute(query14)
rows = cursor.fetchone()
print(rows[0], 'is the average age for class 1 passengers')

print('_______________________________________')


#What was the average fare by passenger class? By survival?

print('What was the average fare for class 3 passengers?')

query15 = """
SELECT AVG(fare) FROM titanic_table
WHERE pclass = 3
"""
cursor.execute(query15)
rows = cursor.fetchone()
print(rows[0], 'was the average fare for class 3 passengers')

print('_______________________________________')

print('What was the average fare for class 2 passengers?')

query16 = """
SELECT AVG(fare) FROM titanic_table
WHERE pclass = 2
"""
cursor.execute(query16)
rows = cursor.fetchone()
print(rows[0], 'was the average fare for class 2 passengers')

print('_______________________________________')

print('What was the average fare for class 1 passengers?')

query17 = """
SELECT AVG(fare) FROM titanic_table
WHERE pclass = 1
"""
cursor.execute(query17)
rows = cursor.fetchone()
print(rows[0], 'was the average fare for class 1 passengers')

print('_______________________________________')

print('What was the average fare for survivors?')

query18 = """
SELECT AVG(fare) FROM titanic_table
WHERE survived > 0
"""
cursor.execute(query18)
rows = cursor.fetchone()
print(rows[0], 'was the average fare for survivors')

print('_______________________________________')

print('What was the average fare for nonsurvivors?')

query19 = """
SELECT AVG(fare) FROM titanic_table
WHERE survived < 1
"""
cursor.execute(query19)
rows = cursor.fetchone()
print(rows[0], 'was the average fare for nonsurvivors')

print('_______________________________________')

print('What was the average number of siblings/spouses aboard for class 3?')

query20 = """
SELECT AVG(sib_spouses_count) FROM titanic_table
WHERE pclass = 3
"""
cursor.execute(query20)
rows = cursor.fetchone()
print(rows[0], 'was the average number siblings/spouses aboard in class 3')
print('_______________________________________')

print('What was the average number of parents/children aboard for class 2?')

query21 = """
SELECT AVG(sib_spouses_count) FROM titanic_table
WHERE pclass = 2
"""
cursor.execute(query21)
rows = cursor.fetchone()
print(rows[0], 'was the average number siblings/spouses aboard in class 2')
print('_______________________________________')

print('What was the average number of parents/children aboard for class 1?')

query22 = """
SELECT AVG(sib_spouses_count) FROM titanic_table
WHERE pclass = 1
"""
cursor.execute(query22)
rows = cursor.fetchone()
print(rows[0], 'was the average number siblings/spouses aboard in class 1')
print('_______________________________________')

print('What was the average number of siblings/spouses aboard who survived?')

query23 = """
SELECT AVG(sib_spouses_count) FROM titanic_table
WHERE survived > 0
"""
cursor.execute(query23)
rows = cursor.fetchone()
print(rows[0], 'was the average number siblings/spouses aboard who survived')
print('_______________________________________')

print('What was the average number of siblings/spouses aboard who did not survive?')

query24 = """
SELECT AVG(sib_spouses_count) FROM titanic_table
WHERE survived < 1
"""
cursor.execute(query24)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard who did not survive')
print('_______________________________________')

print('What was the average number of parents/children aboard for class 3?')                   

query25 = """
SELECT AVG(parent_child_count) FROM titanic_table
WHERE pclass = 3
"""
cursor.execute(query25)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard in class 3')
print('_______________________________________')

print('What was the average number of parents/children aboard for class 2?')

query26 = """
SELECT AVG(parent_child_count) FROM titanic_table
WHERE pclass = 2
"""
cursor.execute(query26)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard in class 2')
print('_______________________________________')

print('What was the average number of parents/children aboard for class 1?')

query27 = """
SELECT AVG(parent_child_count) FROM titanic_table
WHERE pclass = 1
"""
cursor.execute(query27)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard in class 1')
print('_______________________________________')

print('What was the average number of parents/children aboard who survived?')

query28 = """
SELECT AVG(parent_child_count) FROM titanic_table
WHERE survived > 0
"""
cursor.execute(query28)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard who survived')
print('_______________________________________')

print('What was the average number of parents/children aboard who did not survive?')

query29 = """
SELECT AVG(parent_child_count) FROM titanic_table
WHERE survived < 1
"""
cursor.execute(query29)
rows = cursor.fetchone()
print(rows[0], 'was the average number parents/children aboard who did not survive')
print('_______________________________________')

print("How many passengers have the same name?")

query31 = """
SELECT name FROM titanic_table
GROUP BY name 
HAVING(COUNT(name) > 1)
"""

cursor.execute(query31)
rows = cursor.fetchone()
print(rows)


