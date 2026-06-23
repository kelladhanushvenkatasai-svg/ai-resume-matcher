import pandas as pd
import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level = logging.INFO, format= '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

conn =psycopg2.connect(
    host ='localhost',
    database ='resumematcher',
    user = 'postgres',
    password ='postgres123'
)
# jobs by role
query1= """
SELECT search_role , count(*) as job_count
FROM jobs
GROUP BY search_role
ORDER BY job_count DESC;
"""
#Query 2: Most common words in descriptions (proxy for skills)
query2 ="""
SELECT 
    job_id,
    title,
    company,
    search_role,
    LENGTH(description) as description_length
FROM jobs
ORDER BY description_length DESC
LIMIT 10;
"""

# Query 3: Jobs by source
query3 = """
SELECT source, COUNT(*) as count
FROM jobs
GROUP BY source
ORDER BY count DESC;
"""

# Query 4: Top companies hiring
query4 = """
SELECT company, COUNT(*) as job_count
FROM jobs
GROUP BY company
ORDER BY job_count DESC
LIMIT 10;
"""




logger.info("=" * 50)
logger.info("JOB MARKET ANALYTICS")
logger.info("=" * 50)


logger.info("\n1. JOBS BY ROLE")
df1 = pd.read_sql(query1, conn)
print(df1.to_string(index=False))

logger.info("\n 2. Description")
df2 = pd.read_sql(query2,conn)
print(df2.to_string(index = False))

# Execute Query 3
logger.info("\n3. JOBS BY SOURCE")
df3 = pd.read_sql(query3, conn)
print(df3.to_string(index=False))

# Execute Query 4
logger.info("\n4. TOP 10 COMPANIES HIRING")
df4 = pd.read_sql(query4, conn)
print(df4.to_string(index=False))


logger.info("\n" + "=" * 50)
logger.info("TOTAL JOBS IN DATABASE")
total_query = "SELECT COUNT(*) as total FROM jobs;"
total = pd.read_sql(total_query, conn)
print(f"Total: {total['total'].values[0]}")

conn.close()
logger.info("=" * 50)
