# import os
# import re
# import json
# import shutil
# from PyPDF2 import PdfReader
# from fastapi import FastAPI, UploadFile, File, Form, HTTPException

# app = FastAPI()

# # Directory to store incoming files
# STORAGE_PATH = r"..\airflow\dags\data"
# os.makedirs(STORAGE_PATH, exist_ok=True)

# # Function to pull details from uploaded PDF
# def parse_pdf_info(pdf_file):
#     try:
#         reader = PdfReader(pdf_file)
#         content = ""
#         for page in reader.pages:
#             txt = page.extract_text()
#             if txt:
#                 content += txt
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"PDF reading failed: {err}")

#     try:
#         # Identify type of document
#         doc_kind = "INSURANCE_RECEIPT" if "LIC" in content else "UNCLASSIFIED"
#         # Capture premium amount
#         premium_search = re.search(r"Premium Amount[:\s]*₹?(\d+)", content)
#         premium_value = premium_search.group(1) if premium_search else "NOT_FOUND"
#         # Capture submission date
#         date_search = re.search(r"Submission Date[:\s]*(\d{2}-\d{2}-\d{4})", content)
#         submitted_date = date_search.group(1) if date_search else "NOT_FOUND"
#         # Capture financial year (format: YYYY-YYYY)
#         fy_search = re.search(r"Financial Year[:\s]*(\d{4}-\d{4})", content)
#         fy_period = fy_search.group(1) if fy_search else "NOT_FOUND"
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"PDF data extraction failed: {err}")

#     return doc_kind, premium_value, submitted_date, fy_period

# # API to receive file and validate provided financial year
# @app.post("/submit")
# async def submit_receipt(
#     file: UploadFile = File(...),
#     fy_input: str = Form(...)
# ):
#     saved_pdf = os.path.join(STORAGE_PATH, "insurance_receipt.pdf")

#     # Save uploaded PDF locally
#     try:
#         with open(saved_pdf, "wb") as buffer:
#             shutil.copyfileobj(file.file, buffer)
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"Unable to store uploaded file: {err}")

#     # Extract and validate PDF contents
#     try:
#         doc_kind, premium_value, submitted_date, fy_from_pdf = parse_pdf_info(saved_pdf)
#     except HTTPException as e:
#         raise e
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"Error extracting information from PDF: {err}")

#     # Check consistency of financial year
#     if fy_input != fy_from_pdf:
#         raise HTTPException(
#             status_code=400,
#             detail=f"Financial year mismatch detected — Input: {fy_input}, PDF: {fy_from_pdf}"
#         )

#     # Save metadata as JSON
#     info_dict = {
#         "financial_year": fy_input,
#         "document_type": doc_kind,
#         "premium_amount": premium_value,
#         "submission_date": submitted_date
#     }
#     meta_json = os.path.join(STORAGE_PATH, "document_info.json")

#     try:
#         with open(meta_json, "w") as meta_file:
#             json.dump(info_dict, meta_file)
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"Error saving metadata file: {err}")

#     return {"message": "Upload and metadata storage successful"}


# DATA_PATH = r"..\airflow\dags\data"

# def convert_pdfs_to_json(pdf_dir=DATA_PATH):
#     """
#     Reads all PDFs in a directory, extracts key fields, and saves
#     a JSON file for each PDF with the same base name.
#     """
#     # Ensure the folder exists
#     if not os.path.exists(pdf_dir):
#         raise FileNotFoundError(f"Directory not found: {pdf_dir}")

#     # Loop through all PDF files
#     for filename in os.listdir(pdf_dir):
#         if filename.lower().endswith(".pdf"):
#             pdf_path = os.path.join(pdf_dir, filename)
#             try:
#                 # Read PDF content
#                 reader = PdfReader(pdf_path)
#                 content = ""
#                 for page in reader.pages:
#                     txt = page.extract_text()
#                     if txt:
#                         content += txt

#                 # Extract fields
#                 doc_kind = "INSURANCE_RECEIPT" if "LIC" in content else "UNCLASSIFIED"
#                 premium_match = re.search(r"Premium Amount[:\s]*₹?(\d+)", content)
#                 premium_value = premium_match.group(1) if premium_match else "NOT_FOUND"
#                 date_match = re.search(r"Submission Date[:\s]*(\d{2}-\d{2}-\d{4})", content)
#                 submitted_date = date_match.group(1) if date_match else "NOT_FOUND"
#                 fy_match = re.search(r"Financial Year[:\s]*(\d{4}-\d{4})", content)
#                 fy_period = fy_match.group(1) if fy_match else "NOT_FOUND"

#                 # Build JSON dictionary
#                 info_dict = {
#                     "pdf_file": filename,
#                     "document_type": doc_kind,
#                     "premium_amount": premium_value,
#                     "submission_date": submitted_date,
#                     "financial_year": fy_period
#                 }

#                 # Save JSON file with same base name
#                 json_filename = os.path.splitext(filename)[0] + ".json"
#                 json_path = os.path.join(pdf_dir, json_filename)
#                 with open(json_path, "w") as json_file:
#                     json.dump(info_dict, json_file, indent=4)

#                 print(f"✅ Processed {filename} -> {json_filename}")

#             except Exception as e:
#                 print(f"✖ Failed to process {filename}: {e}")

# # Default root endpoint
# @app.get("/")
# def root_response():
#     return {"message": "Receipt verification service is active"}


import os
import re
import json
import shutil
from PyPDF2 import PdfReader
from fastapi import FastAPI, UploadFile, File, Form, HTTPException

app = FastAPI()

# Directory to store incoming files
STORAGE_PATH = r"..\airflow\dags\data"
os.makedirs(STORAGE_PATH, exist_ok=True)


# Function to pull details from a single PDF
def parse_pdf_info(pdf_file):
    try:
        reader = PdfReader(pdf_file)
        content = ""
        for page in reader.pages:
            txt = page.extract_text()
            if txt:
                content += txt
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"PDF reading failed: {err}")

    try:
        # Extract key fields
        doc_kind = "INSURANCE_RECEIPT" if "LIC" in content else "UNCLASSIFIED"
        premium_search = re.search(r"Premium Amount[:\s]*₹?(\d+)", content)
        premium_value = premium_search.group(1) if premium_search else "NOT_FOUND"
        date_search = re.search(r"Submission Date[:\s]*(\d{2}-\d{2}-\d{4})", content)
        submitted_date = date_search.group(1) if date_search else "NOT_FOUND"
        fy_search = re.search(r"Financial Year[:\s]*(\d{4}-\d{4})", content)
        fy_period = fy_search.group(1) if fy_search else "NOT_FOUND"
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"PDF data extraction failed: {err}")

    return doc_kind, premium_value, submitted_date, fy_period


# API to upload PDF and validate the provided financial year
@app.post("/submit")
async def submit_receipt(file: UploadFile = File(...), fy_input: str = Form(...)):
    saved_pdf = os.path.join(STORAGE_PATH, file.filename)

    # Save uploaded PDF
    try:
        with open(saved_pdf, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Unable to store uploaded file: {err}")

    # Extract PDF data
    try:
        doc_kind, premium_value, submitted_date, fy_from_pdf = parse_pdf_info(saved_pdf)
    except HTTPException as e:
        raise e
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Error extracting info: {err}")

    # Check financial year consistency
    if fy_input != fy_from_pdf:
        raise HTTPException(
            status_code=400,
            detail=f"Financial year mismatch — Input: {fy_input}, PDF: {fy_from_pdf}"
        )

    # Save metadata as JSON
    info_dict = {
        "financial_year": fy_input,
        "document_type": doc_kind,
        "premium_amount": premium_value,
        "submission_date": submitted_date
    }
    json_filename = os.path.splitext(file.filename)[0] + ".json"
    meta_json = os.path.join(STORAGE_PATH, json_filename)

    try:
        with open(meta_json, "w") as meta_file:
            json.dump(info_dict, meta_file, indent=4)
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Error saving metadata: {err}")

    return {"message": f"Upload successful, metadata saved as {json_filename}"}


# Function to convert all PDFs in storage folder to JSON
def convert_pdfs_to_json(pdf_dir=STORAGE_PATH):
    if not os.path.exists(pdf_dir):
        raise FileNotFoundError(f"Directory not found: {pdf_dir}")

    for filename in os.listdir(pdf_dir):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(pdf_dir, filename)
            try:
                reader = PdfReader(pdf_path)
                content = ""
                for page in reader.pages:
                    txt = page.extract_text()
                    if txt:
                        content += txt

                doc_kind = "INSURANCE_RECEIPT" if "LIC" in content else "UNCLASSIFIED"
                premium_match = re.search(r"Premium Amount[:\s]*₹?(\d+)", content)
                premium_value = premium_match.group(1) if premium_match else "NOT_FOUND"
                date_match = re.search(r"Submission Date[:\s]*(\d{2}-\d{2}-\d{4})", content)
                submitted_date = date_match.group(1) if date_match else "NOT_FOUND"
                fy_match = re.search(r"Financial Year[:\s]*(\d{4}-\d{4})", content)
                fy_period = fy_match.group(1) if fy_match else "NOT_FOUND"

                info_dict = {
                    "pdf_file": filename,
                    "document_type": doc_kind,
                    "premium_amount": premium_value,
                    "submission_date": submitted_date,
                    "financial_year": fy_period
                }

                json_filename = os.path.splitext(filename)[0] + ".json"
                json_path = os.path.join(pdf_dir, json_filename)
                with open(json_path, "w") as json_file:
                    json.dump(info_dict, json_file, indent=4)

                print(f"✅ {filename} -> {json_filename}")

            except Exception as e:
                print(f"✖ Failed {filename}: {e}")


# Optional API endpoint to convert all PDFs to JSON via request
@app.post("/convert_all")
def convert_all_pdfs():
    try:
        convert_pdfs_to_json()
        return {"message": "All PDFs processed and JSON files created successfully."}
    except Exception as err:
        raise HTTPException(status_code=500, detail=str(err))


# Root endpoint
@app.get("/")
def root_response():
    return {"message": "Receipt verification service is active"}
