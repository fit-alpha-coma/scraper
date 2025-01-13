import csv
from jobspy import scrape_jobs
import pandas as pd
# import uk-11-1.csv
uk_jobs = pd.read_csv("US.csv")
company_name = uk_jobs["Company"].unique()

jobs_list = []
# in for loop, store each one in same csv file
for company in company_name:
    jobs = scrape_jobs(
        site_name=["indeed", "zip_recruiter", "glassdoor", "google"],
        search_term=f'Company: "{company}"',
        google_search_term=f'Company: "{company}" job postings in United States',
        country_indeed='United States',
        location="United States",
        hours_old=720,
    )
    # add company name to jobs
    jobs["company"] = company
    if "description" in jobs.columns:
        jobs = jobs.drop(columns=['description'])
    if "company_description" in jobs.columns:
        jobs = jobs.drop(columns=['company_description'])
    jobs_list.append(jobs)

# combine all jobs
jobs = pd.concat(jobs_list)
print(f"Found {len(jobs)} jobs")
print(jobs.head())
jobs.to_csv("us_final.csv", quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False) # to_excel
