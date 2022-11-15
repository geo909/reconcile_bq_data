from dotenv import load_dotenv
from pathlib import Path
import json
import os

dot_env_path = Path(__file__).parent.parent.parent / ".env"
if dot_env_path.is_file():
    load_dotenv()

MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PORT = 3306
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]
MYSQL_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
)

BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_CREDENTIALS_INFO = json.loads(os.environ["GCP_KEY_TZANAKIS_BIGQUERY"])
