# Import required modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import re
import logging
from PyPDF2 import PdfReader

# Folder paths
DATA_DIR = "/opt/airflow/dags/data"
RECEIPT_FILE = os.path.join(DATA_DIR, "Sample_data.pdf")
INFO_FILE = os.path.join(DATA_DIR, "Sample_data.json")

# Extract financial year range
def derive_fy_range(details):
    fy_string = details.get("financial_year")
    if not fy_string:
        logging.error("✖ Missing key 'financial_year' in metadata.")
        return None, None

    # Normalize separators and remove 'FY' prefix if present
    fy_string = fy_string.strip().upper().replace("FY", "").replace("/", "-").strip()

    # Match patterns like '2024-2025' or '2024-25'
    match = re.match(r"(\d{4})-(\d{2,4})", fy_string)
    if not match:
        logging.error(f"✖ Invalid format for financial year: {fy_string}")
        return None, None

    start_year = int(match.group(1))
    end_year_part = match.group(2)
    
    # Handle short form like '2024-25'
    if len(end_year_part) == 2:
        end_year = (start_year // 100) * 100 + int(end_year_part)
    else:
        end_year = int(end_year_part)

    fy_start_date = datetime(start_year, 4, 1)
    fy_end_date = datetime(end_year, 4, 1)
    return fy_start_date, fy_end_date


# Read text from a PDF
def extract_pdf_content(pdf_file):
    try:
        reader = PdfReader(pdf_file)
        content = ""
        for page in reader.pages:
            content += page.extract_text() or ""
        return content
    except Exception as err:
        logging.error(f"✖ Unable to read PDF: {err}")
        return None

# Extract key data points from text
def parse_receipt_data(content):
    doc_category = "INSURANCE_RECEIPT" if "LIC" in content else "UNIDENTIFIED"
    premium_val = re.search(r"Premium Amount[:\s]*₹?(\d+)", content)
    date_val = re.search(r"Submission Date[:\s]*(\d{2}-\d{2}-\d{4})", content)

    if not premium_val or not date_val:
        logging.error("✖ Missing required fields in the document.")
        return None, None, None

    premium_amount = premium_val.group(1)
    try:
        submit_date = datetime.strptime(date_val.group(1), "%d-%m-%Y")
    except ValueError:
        logging.error("✖ Incorrect submission date format.")
        return None, None, None

    return doc_category, premium_amount, submit_date

# Validate the document type
def confirm_doc_type(doc_category):
    if doc_category != "INSURANCE_RECEIPT":
        logging.error("✖ The provided document is not a valid LIC Receipt.")
        return False
    return True

# Validate the submission date against FY range
def check_submission_period(submit_date, fy_start, fy_end, amount, fy_label):
    if fy_start <= submit_date < fy_end:
        message = f"Premium ₹{amount} is valid for FY {fy_label}"
    else:
        message = f"Submission date {submit_date.strftime('%d-%m-%Y')} does not fall in FY {fy_label}"
    logging.info(message)
    return message

# Main callable function for DAG task
def process_receipt_data(**kwargs):
    try:
        with open(INFO_FILE, "r") as f:
            info = json.load(f)

        fy_start, fy_end = derive_fy_range(info)
        if not (fy_start and fy_end):
            return

        content = extract_pdf_content(RECEIPT_FILE)
        if not content:
            return

        doc_type, amount, date = parse_receipt_data(content)
        if not all([doc_type, amount, date]):
            return

        if not confirm_doc_type(doc_type):
            return

        fy_label = info.get("financial_year")
        return check_submission_period(date, fy_start, fy_end, amount, fy_label)

    except FileNotFoundError as e:
        logging.error(f"✖ File not found: {e}")
    except json.JSONDecodeError:
        logging.error("✖ Failed to read or decode JSON metadata.")
    except Exception as e:
        logging.error(f"✖ Unexpected error occurred: {e}")

# Default DAG arguments
default_params = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'owner': 'airflow'
}

# DAG Definition
with DAG(
    dag_id="insurance_receipt_handler",
    default_args=default_params,
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["insurance", "pdf", "etl"]
) as dag:

    handle_receipt = PythonOperator(
        task_id="handle_receipt",
        python_callable=process_receipt_data
    )

    handle_receipt
