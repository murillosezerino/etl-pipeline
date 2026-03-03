import os
from dotenv import load_dotenv

load_dotenv()

R2_ENDPOINT          = os.getenv("R2_ENDPOINT")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_RAW_BUCKET        = os.getenv("R2_RAW_BUCKET", "etl-pipeline-raw")
R2_PROCESSED_BUCKET  = os.getenv("R2_PROCESSED_BUCKET", "etl-pipeline-processed")
LOG_LEVEL            = os.getenv("LOG_LEVEL", "INFO")