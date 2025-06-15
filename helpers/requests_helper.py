import os
import requests
from datetime import datetime, timedelta

from .logger import get_logger
from dotenv import load_dotenv
load_dotenv()
logging = get_logger()

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

def generate_filename(name: str, type: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"{date_str}_{name}.{type}"
    return filename


def fetch_and_save_json(url: str, params: dict, headers: dict, filename: str = "response.json") -> bool:
    """
    Fetch JSON data from API and save to a file
    """
    logging.info(f"Sending GET request to {url} with params {params} and headers keys {list(headers.keys())}")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return False

    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        logging.info(f"Response saved to {filename}")
        return True
    except Exception as e:
        logging.error(f"Failed to save response to file: {e}")
        return False


def build_earnings_params(start_date, end_date, extra=None):
    """
    Build params for earnings request
    """
    logging.info(f"Building params for earnings request")
    params = {
        "report": "earnings",
        "v3": "",
        "list": "",
        "start": start_date,
        "end": end_date,
        "breakdown_level": "earnings",
        "structure": "flat",
        "export": "json"
    }
    if extra:
        params.update(extra)
    return params


def get_last_month_date_range():
    """
    Get dates for last month
    """
    today = datetime.now()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return first_day_last_month.strftime("%Y-%m-%d"), last_day_last_month.strftime("%Y-%m-%d")


def fetch_earnings_month_report():
    start_date, end_date = get_last_month_date_range()
    params = build_earnings_params(start_date=start_date, end_date=end_date)
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    filename = generate_filename("earnings", 'json')
    success = fetch_and_save_json(API_URL, params, headers, filename)

    if success:
        logging.info(f"Report saved successfully as {filename}")
        return filename
    else:
        logging.error("Failed to fetch or save the report")
        return None

def mock_fetch_earnings_month_report():
    logging.info(f"Sending GET request to API to get monthly earnings report")
    filename = generate_filename("earnings", 'json')
    success = True

    if success:
        logging.info(f"Report saved successfully as {filename}")
        return filename
    else:
        logging.error("Failed to fetch or save the report")
        return None