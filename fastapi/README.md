# Airflow + FastAPI Project Setup

This  will make a clear understanding about  set up  for the FastAPI backend and Airflow environment for handling LIC receipt files



# Prerequisites
- Python or Anaconda installed
- Docker and Docker Compose installed 




# Folder Structure Overview

AirFlow/
 airFlow/
   config/
    dags/
      input/
     logs/
      plugins/
    docker-compose.yaml
      dockerfile
       .env
     fastapi/
        main.py
       requirements.txt



## Step 1: FastAPI Backend Setup

1. Navigate to the 'fastapi' folder 

2. Create `requirements.txt` and add

3. Create and activate a virtual environment

4. Install dependencies

5. Run FastAPI server

7. Test the `/upload` endpoint
- Click Try it out
- Upload a .pdf` file and provide the year in `YYYY-YYYY` format
- Click Execute

8. Verify output 
metadata.json and lic_receipt.pdf will be created in 



