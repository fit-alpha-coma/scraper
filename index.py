import csv
from jobspy import scrape_jobs
import pandas as pd
import time
import os
from datetime import datetime

def load_progress():
    """Load previously processed companies and existing jobs"""
    if os.path.exists("progress.csv"):
        progress_df = pd.read_csv("progress.csv")
        processed_companies = set(progress_df["company"].unique())
    else:
        # Create progress file with proper columns
        progress_df = pd.DataFrame(columns=['title', 'company', 'location', 'posted_at', 'last_updated'])
        progress_df.to_csv("progress.csv", index=False)
        processed_companies = set()
    return progress_df, processed_companies

def save_batch(jobs_list, filename="us_final.csv", batch_filename="latest_batch.csv"):
    """Save current batch and update main file"""
    try:
        # Save latest batch separately (for recovery)
        batch_df = pd.concat(jobs_list, ignore_index=True)
        batch_df.to_csv(batch_filename, index=False)
        
        # Update main file
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
            # Remove duplicates based on job title and company
            combined_df = combined_df.drop_duplicates(subset=['title', 'company', 'location'], keep='last')
        else:
            combined_df = batch_df
            
        combined_df.to_csv(filename, index=False)
        print(f"Successfully saved {len(batch_df)} new jobs. Total jobs: {len(combined_df)}")
        return True
    except Exception as e:
        print(f"Error saving data: {str(e)}")
        return False

def save_progress(jobs_df, filename="progress.csv"):
    """Save job data to progress file"""
    try:
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined_df = pd.concat([existing_df, jobs_df], ignore_index=True)
            # Remove duplicates based on job title and company
            combined_df = combined_df.drop_duplicates(subset=['title', 'company', 'location'], keep='last')
        else:
            combined_df = jobs_df
        
        combined_df.to_csv(filename, index=False)
        return True
    except Exception as e:
        print(f"Error saving progress: {str(e)}")
        return False

def main():
    # Load existing progress
    existing_jobs, processed_companies = load_progress()
    
    # Read company list
    uk_jobs = pd.read_csv("netherlands.csv")
    company_name = uk_jobs["company"].unique()
    
    # Filter out already processed companies
    companies_to_process = [c for c in company_name if c not in processed_companies]
    print(f"Found {len(companies_to_process)} companies to process out of {len(company_name)} total")
    
    jobs_list = []
    batch_size = 40  # Save every 40 companies
    
    for index, company in enumerate(companies_to_process, 1):
        try:
            print(f"Processing company {index}/{len(companies_to_process)}: {company}")
            
            # Check if we already have recent data for this company
            if os.path.exists("us_final.csv"):
                existing_company_jobs = pd.read_csv("us_final.csv")
                company_last_job = existing_company_jobs[existing_company_jobs['company'] == company]
                if not company_last_job.empty:
                    last_update = pd.to_datetime(company_last_job['posted_at']).max()
                    if (datetime.now() - last_update).days < 30:  # Skip if data is less than 30 days old
                        print(f"Skipping {company} - recent data exists")
                        continue
            
            jobs = scrape_jobs(
                site_name="linkedin",
                search_term=f'{company}',
                google_search_term=f'Company: "{company}" job postings in Netherlands',
                country_indeed='Netherlands',
                location="Netherlands",
                hours_old=720,
            )
            print(jobs)
            
            if not jobs.empty:
                # Add metadata
                jobs["company"] = company
                jobs["last_updated"] = datetime.now().isoformat()
                
                # Drop unnecessary columns
                for col in ['description', 'company_description']:
                    if col in jobs.columns:
                        jobs = jobs.drop(columns=[col])
                
                jobs_list.append(jobs)
                
                # Save both progress and batch every batch_size companies
                if index % batch_size == 0:
                    print(f"Saving batch at {index} companies...")
                    batch_df = pd.concat(jobs_list, ignore_index=True)
                    
                    # Save to progress file
                    save_progress(batch_df)
                    
                    # Save to main file
                    if save_batch(jobs_list):
                        jobs_list = []  # Clear list after successful save
                    
                    print(f"Pausing for 2 seconds...")
                    time.sleep(2)
                
                # Quick save of current job data
                save_progress(jobs)
                
        except Exception as e:
            print(f"Error processing company {company}: {str(e)}")
            # Save progress even on error
            if jobs_list:
                error_df = pd.concat(jobs_list, ignore_index=True)
                save_progress(error_df, filename="error_progress.csv")
                save_batch(jobs_list, filename="error_backup.csv")
            continue
    
    # Final save for remaining jobs
    if jobs_list:
        final_batch = pd.concat(jobs_list, ignore_index=True)
        save_progress(final_batch)
        save_batch(jobs_list)
    
    # Combine and deduplicate all data
    final_cleanup()

def final_cleanup():
    """Combine all data and remove duplicates"""
    try:
        main_df = pd.read_csv("us_final.csv")
        if os.path.exists("error_backup.csv"):
            error_df = pd.read_csv("error_backup.csv")
            main_df = pd.concat([main_df, error_df], ignore_index=True)
        
        # Remove duplicates
        main_df = main_df.drop_duplicates(subset=['title', 'company', 'location'], keep='last')
        
        # Sort by date
        if 'posted_at' in main_df.columns:
            main_df['posted_at'] = pd.to_datetime(main_df['posted_at'])
            main_df = main_df.sort_values('posted_at', ascending=False)
        
        main_df.to_csv("us_final.csv", index=False)
        print("Final cleanup completed successfully")
    except Exception as e:
        print(f"Error during final cleanup: {str(e)}")

if __name__ == "__main__":
    main()