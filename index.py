from jobspy import scrape_jobs
import requests
import os
import pandas as pd
import time
from requests.exceptions import RequestException
import logging
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
OUTPUT_DIR = ""
def get_company_linkedin_id(company_name: str) -> str | None:
    """Get LinkedIn company ID with error handling and rate limiting."""
    try:
        # URL encode company name to handle special characters
        url = f'https://www.linkedin.com/jobs-guest/api/typeaheadHits?typeaheadType=COMPANY&query={company_name}'

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise exception for bad status codes

        data = response.json()

        if not data or len(data) == 0:
            logger.warning(f"No company found for: {company_name}")
            return None

        company_id = data[0]['id']
        return company_id

    except requests.exceptions.JSONDecodeError:
        logger.error(f"Failed to decode JSON for company: {company_name}")
        save_error_records(company_name, "JSONDecodeError: Failed to decode JSON", OUTPUT_DIR)
        return None
    except RequestException as e:
        logger.error(f"Request failed for company {company_name}: {str(e)}")
        save_error_records(company_name, f"RequestException: {str(e)}", OUTPUT_DIR)
        return None
    except Exception as e:
        logger.error(f"Unexpected error for company {company_name}: {str(e)}")
        save_error_records(company_name, f"UnexpectedError: {str(e)}", OUTPUT_DIR)
        return None

def scrape_company_linkedin_jobs(company_name: str, location: str, fallback_company_name: str | None =None):
    """Scrape jobs with error handling and fallback to another company name."""
    try:
        # Attempt to scrape jobs with the provided company name
        company_id = get_company_linkedin_id(company_name)
        if company_id is None:
            logger.warning(f"No LinkedIn ID found for company: {company_name}")
            return pd.DataFrame()

        company_id = int(company_id)
        jobs = scrape_jobs(
            site_name="linkedin",
            linkedin_company_ids=[company_id],
            location=location,
            hours_old=720,
        )

        # If jobs are empty and fallback is available, try with fallback company name
        if jobs.empty and fallback_company_name:
            logger.info(f"No jobs found for {company_name}. Retrying with fallback: {fallback_company_name}")
            company_id = get_company_linkedin_id(fallback_company_name,)
            if company_id is None:
                logger.warning(f"No LinkedIn ID found for fallback company: {fallback_company_name}")
                return pd.DataFrame()

            company_id = int(company_id)
            jobs = scrape_jobs(
                site_name="linkedin",
                linkedin_company_ids=[company_id],
                location=location,
                hours_old=720,
            )

        print(jobs)

        return jobs

    except Exception as e:
        logger.error(f"Error scraping jobs for {company_name}: {str(e)}")
        return pd.DataFrame()

def save_empty_or_none_records(company_name: str, output_dir: str):
    """Save records for companies with no jobs found."""
    try:
        output_file = os.path.join(output_dir, "no_jobs_found.csv")
        df = pd.DataFrame({"Company": [company_name]})
        if not os.path.exists(output_file):
            df.to_csv(output_file, index=False)
        else:
            df.to_csv(output_file, mode='a', header=False, index=False)
    except Exception as e:
        logger.error(f"Error saving no-jobs-found record for {company_name}: {str(e)}")


def save_error_records(company_name: str, error_message: str, output_dir: str):
    """Save records for companies where an error occurred."""
    try:
        output_file = os.path.join(output_dir, "error_records.csv")
        df = pd.DataFrame({"Company": [company_name], "Error": [error_message]})
        if not os.path.exists(output_file):
            df.to_csv(output_file, index=False)
        else:
            df.to_csv(output_file, mode='a', header=False, index=False)
    except Exception as e:
        logger.error(f"Error saving error record for {company_name}: {str(e)}")


def run_through_csv(csv_file, location, output_dir, start_idx=0, end_idx=None):
    """Process CSV with error handling and rate limiting."""
    try:
        OUTPUT_DIR = output_dir
        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return

        df = pd.read_csv(csv_file)

        if 'Company' not in df.columns:
            logger.error("CSV file must contain a 'Company' column")
            return

        total_rows = len(df)
        if end_idx is None or end_idx >= total_rows:
            end_idx = total_rows - 1

        if start_idx < 0 or start_idx > end_idx:
            logger.error(f"Invalid row range: {start_idx}-{end_idx}. Total rows in file: {total_rows}.")
            return

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_file = os.path.join(output_dir, "jobs.csv")

        # Create output file with headers if it doesn't exist
        if not os.path.exists(output_file):
            pd.DataFrame().to_csv(output_file, index=False)

        for idx in range(start_idx, end_idx + 1):
            company = df['Company'].iloc[idx]
            company_fallback = df['Company Name for Emails'].iloc[idx] if 'Company Name for Emails' in df.columns else None
            if pd.isna(company):
                continue

            logger.info(f"Processing company {idx + 1}/{end_idx + 1}: {company}")

            try:
                jobs = scrape_company_linkedin_jobs(company, location, company_fallback)

                if jobs.empty:
                    save_empty_or_none_records(company, output_dir)
                else:
                    # Append without writing index and only write header if file is empty
                    jobs.to_csv(output_file, mode='a', header=False, index=False)

            except Exception as e:
                error_message = str(e)
                logger.error(f"Error processing company {company}: {error_message}")
                save_error_records(company, error_message, output_dir)

            print("\n")
            time.sleep(1)

    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")



def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='LinkedIn Job Scraper with sequential processing')
    parser.add_argument('csv_file', help='Input CSV file containing company names')
    parser.add_argument('location', help='Location to search for jobs')
    parser.add_argument('output_dir', help='Directory to save output files')
    parser.add_argument('--start', type=int, default=0, help='Starting index (0-based, inclusive)')
    parser.add_argument('--end', type=int, help='Ending index (0-based, inclusive)')
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    run_through_csv(
        args.csv_file,
        args.location,
        args.output_dir,
        start_idx=args.start,
        end_idx=args.end
    )
