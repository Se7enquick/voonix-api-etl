import json
import os

from helpers.logger import get_logger
from helpers.google_cloud_helper import load_file_to_gcs, load_parquet_to_bq
from helpers.requests_helper import mock_fetch_earnings_month_report, generate_filename
from dotenv import load_dotenv
import polars as pl

logging = get_logger()
load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
STAGING_DATA_PATH = os.getenv("STAGING_DATA_PATH")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
REPORT_NAME = os.getenv("REPORT_NAME")

def extract() -> str | None:
    """
    First Stage is the API data fetching + json saving, for testing purposes it will be skipped and mock jsons used
    For now hardcoded because we don't have access to API
    """
    json_filename = mock_fetch_earnings_month_report()
    response_json_base_path = f'{RAW_DATA_PATH}/{json_filename}'
    json_local_path = f"data/{response_json_base_path}"
    load_file_to_gcs(json_local_path, GCS_BUCKET, response_json_base_path)
    return json_local_path


def transform(json_local_path: str) -> str | None:
    """
    Transformation includes json unnesting, adding net_revenue_margin_pct, type casting, pushing to GCS staging layer
    """
    with open(json_local_path, "r", encoding="utf-8") as f:
        data_json = json.load(f)
    month_key = next(iter(data_json["data"]))
    records = data_json["data"][month_key]
    df = pl.DataFrame(records)

    logging.info('Unnesting records')

    df = df.with_columns([
        pl.col("currency").struct.field("code").alias("currency_code"),
        pl.col("currency").struct.field("exchange").alias("currency_exchange"),
    ]).drop("currency")

    df = df.with_columns(
        (pl.col("netrevenue") / pl.col("gross_revenue") * 100).alias("net_revenue_margin_pct")
    )

    logging.info('Adding net revenue aggregation')

    cols_to_select = [
        "host",
        "username",
        "brand",
        "campaign",
        "payment_id",
        "product",
        "reward_plan",
        "date",
        "base_currency",
        "deposit_value",
        "REV_income",
        "Extra_fee",
        "bonus",
        "netrevenue",
        "gross_revenue",
        "turnover",
        "deduction",
        "total",
        "CPA_income",
        "currency_code",
        "currency_exchange"
    ]
    df = df.select(cols_to_select)

    logging.info('Casting data types')

    df = df.with_columns([
        pl.col("host").cast(pl.Utf8),
        pl.col("username").cast(pl.Utf8),
        pl.col("brand").cast(pl.Utf8),
        pl.col("campaign").cast(pl.Utf8),
        pl.col("payment_id").cast(pl.Int64),
        pl.col("product").cast(pl.Utf8),
        pl.col("reward_plan").cast(pl.Utf8),
        pl.col("date").cast(pl.Date),
        pl.col("base_currency").cast(pl.Utf8),
        pl.col("deposit_value").cast(pl.Float64),
        pl.col("REV_income").cast(pl.Float64),
        pl.col("Extra_fee").cast(pl.Float64),
        pl.col("bonus").cast(pl.Float64),
        pl.col("netrevenue").cast(pl.Float64),
        pl.col("gross_revenue").cast(pl.Float64),
        pl.col("turnover").cast(pl.Float64),
        pl.col("deduction").cast(pl.Float64),
        pl.col("total").cast(pl.Float64),
        pl.col("CPA_income").cast(pl.Float64),
        pl.col("currency_code").cast(pl.Utf8),
        pl.col("currency_exchange").cast(pl.Float64),
    ])

    df_filename = generate_filename(REPORT_NAME, 'parquet')
    df_base_path = f'{STAGING_DATA_PATH}/{df_filename}'
    df_local_path = f'data/{df_base_path}'
    df.write_parquet(df_local_path)

    logging.info('Saving report to staging directory')

    try:
        load_file_to_gcs(df_local_path, GCS_BUCKET, df_base_path)
        return df_base_path
    except Exception as e:
        raise e

def load(df_path: str) -> str | None:
    try:
        logging.info(f"Loading report to BigQuery")
        load_parquet_to_bq(BQ_DATASET_ID, REPORT_NAME, f'gs://{GCS_BUCKET}/{df_path}')
    except Exception as e:
        logging.error(f"Failed to load report: {e}")


if __name__ == '__main__':
    json_resp = extract()
    transformed_df = transform(json_resp)
    load(transformed_df)