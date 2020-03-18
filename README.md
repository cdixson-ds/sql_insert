##How to set up from scratch


Setup virtual environment:

'''

pipenv --python 3.7
pipenv install python-dotenv psycopg2-binary
pipenv shell

'''

Setup env vars in a ".env" file (using creds from ElephantSQL)

DB_NAME="______________"
DB_USER="______________"
DB_PASSWORD="______________"
DB_HOST="______________"

Run:

'''sh
python app/pg_queries.py
'''