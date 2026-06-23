"""Job Data Ingestion Pipeline
Title : AI Resume Matcher
Author: Dhanush Venkata Sai Kella
Purpose: Fetch live job postings from JSearch API (LinkedIn, Indeed, Glassdoor)
"""

import os
import pandas as pd
import logging
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import numpy as np

#Loading .env files
load_dotenv()

# Logging Setup
logging.basicConfig(
    level = logging.INFO,
    format= "%(asctime)s- %(levelname)s- %(message)s"
)
logger = logging.getLogger(__name__)

#CONFIG

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY_ID")

HEADERS = {
    "X-RapidAPI-Key":  RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
}

BASE_URL = "https://jsearch.p.rapidapi.com/search"

# job roles want to collect
JOB_ROLES=[
    "Data Analyst",
    "Data Scientist"
    "Machine Learning Engineer"
    "AI Engineer"
]

PAGES_PER_ROLE = 2

#Fetching data from Website
def fetch_jobs_for_role(role: str, pages: int) -> list:
    """
    Fetch live job postings for a specific role.
    
    Args:
        role:  Job title to search (e.g. "Data Scientist")
        pages: Number of pages to fetch
    
    Returns:
        List of cleaned job dictionaries
    """
    all_jobs =[]
    for page in range(1, pages+1):
        logger.info(f'fetching {page} for: {role}')
        params = {
             "query" : f"{role} United States",
             "page": str(page),
             "num_pages" : '1',
             'country' : 'us',
             'date_posted': 'today'
        }

        try:
            response = requests.get(BASE_URL,headers=HEADERS,params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            jobs = data.get('data',[])


            for job in jobs:
                clean_job = {
                    "job_id":          job.get("job_id", ""),
                    "title":           job.get("job_title", ""),
                    "company":         job.get("employer_name", ""),
                    "location":        f"{job.get('job_city', '')}, {job.get('job_state', '')}",
                    "description":     job.get("job_description", ""),
                    "employment_type": job.get("job_employment_type", ""),
                    "salary_min":      job.get("job_min_salary", 0),
                    "salary_max":      job.get("job_max_salary", 0),
                    "salary_period":   job.get("job_salary_period", ""),
                    "remote":          job.get("job_is_remote", False),
                    "posted_at":       job.get("job_posted_at_datetime_utc", ""),
                    "apply_url":       job.get("job_apply_link", ""),
                    "source":          job.get("job_publisher", ""),
                    "search_role":     role,
                    "fetched_at":      datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                all_jobs.append(clean_job)

                logger.info(f"Got {len(jobs)} jobs from page {page}")

            # Wait between requests - good API practice
                time.sleep(1)
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API ERROR ON PAGE{page} for {role} : {e}")
            continue
    return all_jobs

# ── CLEAN ────────────────────────────────────────────────────────────────────

def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize raw job data.
    
    Args:
        df: Raw dataframe
    
    Returns:
        Cleaned dataframe
    """
    logger.info("Cleaning data...")

    before = len(df)

    # Remove duplicates
    df = df.drop_duplicates(subset=["job_id"])

    # Remove jobs with empty descriptions
    df = df[df["description"].str.len() > 100]
    # Replace NaN with None for database compatibility
    df = df.replace({np.nan: None})
    
    # Fill missing descriptions with empty string
    df['description'] = df['description'].fillna('')

    # Clean text
    df["title"]       = df["title"].str.strip().str.title()
    df["company"]     = df["company"].str.strip()
    df["location"]    = df["location"].str.strip().str.strip(",")
    df["description"] = df["description"].str.strip()

    # Fix salary
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce").fillna(0)
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce").fillna(0)

    # Reset index
    df = df.reset_index(drop=True)

    after = len(df)
    logger.info(f"Cleaned: {before} → {after} jobs ({before - after} removed)")

    return df


# ── SAVE ─────────────────────────────────────────────────────────────────────

def save_jobs(df: pd.DataFrame) -> str:
    """
    Save cleaned jobs to CSV.
    
    Args:
        df: Cleaned dataframe
    
    Returns:
        Path where file was saved
    """
    os.makedirs("data/processed", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath  = f"data/processed/jobs_{timestamp}.csv"

    df.to_csv(filepath, index=False)
    logger.info(f"Saved {len(df)} jobs → {filepath}")

    return filepath


# ── SUMMARY ──────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame, filepath: str):
    """Print a summary of what we collected."""
    
    print("\n" + "=" * 50)
    print("INGESTION PIPELINE COMPLETE")
    print("=" * 50)
    print(f"Total jobs collected : {len(df)}")
    print(f"Roles covered        : {list(df['search_role'].unique())}")
    print(f"Unique companies     : {df['company'].nunique()}")
    print(f"Unique locations     : {df['location'].nunique()}")
    print(f"Remote jobs          : {df['remote'].sum()}")
    print(f"Jobs with salary     : {(df['salary_min'] > 0).sum()}")
    print(f"Saved to             : {filepath}")
    print("=" * 50)


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info("Starting Job Ingestion Pipeline")
    logger.info("=" * 50)

    # Validate API key
    if not RAPIDAPI_KEY:
        logger.error("RAPIDAPI_KEY missing in .env file!")
        return

    # Step 1 - Fetch
    all_jobs = []
    for role in JOB_ROLES:
        logger.info(f"\nFetching: {role}")
        jobs = fetch_jobs_for_role(role, PAGES_PER_ROLE)
        all_jobs.extend(jobs)
        logger.info(f"Running total: {len(all_jobs)} jobs")

    if not all_jobs:
        logger.error("No jobs fetched. Check your API key.")
        return

    # Step 2 - Dataframe
    df = pd.DataFrame(all_jobs)
    logger.info(f"Raw data: {df.shape[0]} rows x {df.shape[1]} columns")

    # Step 3 - Clean
    df_clean = clean_jobs(df)

    # Step 4 - Save
    filepath = save_jobs(df_clean)

    # Step 5 - Summary
    print_summary(df_clean, filepath)


if __name__ == "__main__":
    main()
        

        
        







