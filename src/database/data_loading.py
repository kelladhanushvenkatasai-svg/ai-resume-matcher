"""Load jobs into Postgres from CSV file
"""

import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level = logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#data base connection

conn = psycopg2.connect(
    host = 'localhost',
    database ='resumematcher',
    user = 'postgres',
    password = 'postgres123'
)

cursor =conn.cursor()

#finding latest .csv file

csv_files = sorted([f for f in os.listdir('data/processed') if f.endswith('.csv')])
if not csv_files:
    logger.error('No CSV file found in the directory')
    exit()
latest_csv = f"data/processed/{csv_files[-1]}"
logger.info(f'{latest_csv} is loading....')
# read csv
df = pd.read_csv(latest_csv)


#Insert into postgres

query = query = """
INSERT INTO jobs (job_id, title, company, description, posted_at, apply_url, source, search_role)
VALUES %s
ON CONFLICT (job_id) DO NOTHING
"""

data = []
for _, row in df.iterrows():
    data.append((
        row['job_id'],
        row['title'],
        row['company'],
        row['description'],
        row['posted_at'],
        row['apply_url'],
        row['source'],
        row['search_role']
    ))

try:
   execute_values(cursor,query,data, page_size=50)
   conn.commit()
   logger.info(f'load {len(data) }jobs into database')
   logger.info('DONE!')
except Exception as e:
   logger.error(f'error loading jobs : {e}')
   conn.rollback()
finally:
   cursor.close()
   conn.close()




   



                                                                  
