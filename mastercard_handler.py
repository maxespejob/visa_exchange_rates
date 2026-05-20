import io
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET     = os.environ.get("S3_BUCKET",     "itl-0004-itx-dev-poc-02-reference")
S3_PREFIX     = os.environ.get("S3_PREFIX",     "exchange-rates/brand=Mastercard")
FUNCTION_NAME = os.environ.get("FUNCTION_NAME", "itl-0004-itx-dev-mastercard-exchange-rates")
BEGIN_DATE    = os.environ.get("BEGIN_DATE",     datetime.now().strftime("%Y-%m-%d"))
END_DATE      = os.environ.get("END_DATE",       datetime.now().strftime("%Y-%m-%d"))

NUM_CHUNKS      = 7
MAX_WORKERS     = 7
REQUEST_TIMEOUT = 2
PAUSE_MIN       = 0.9
PAUSE_MAX       = 1.2

REQUEST_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
    "Accept-Language": "es,es-ES;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,es-PE;q=0.5",
    "sec-fetch-mode":  "navigate",
    "sec-fetch-site":  "none",
    "purpose":         "prefetch",
}

MASTERCARD_CURRENCIES_URL = (
    "https://www.mastercard.com/settlement/currencyrate/settlement-currencies"
)
MASTERCARD_RATES_URL = (
    "https://www.mastercard.com/marketingservices/public/mccom-services/"
    "currency-conversions/conversion-rates"
)

DATE_FORMAT_INPUT  = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%m/%d/%Y"
DATE_FORMAT_FILE   = "%Y%m%d"

# =============================================================================
# HELPERS
# =============================================================================

def generate_date_range(begin_date_str: str, end_date_str: str) -> list[str]:
    """Returns a list of dates in MM/DD/YYYY format between two YYYY-MM-DD dates."""
    try:
        begin = datetime.strptime(begin_date_str, DATE_FORMAT_INPUT)
        end   = datetime.strptime(end_date_str,   DATE_FORMAT_INPUT)
        dates = [
            (begin + timedelta(days=i)).strftime(DATE_FORMAT_OUTPUT)
            for i in range((end - begin).days + 1)
        ]
        logger.info(f"[generate_date_range] {len(dates)} date(s) generated: {dates[0]} -> {dates[-1]}")
        return dates
    except ValueError as e:
        logger.error(f"[generate_date_range] Invalid date format: {e}")
        raise


def split_into_chunks(items: list, num_chunks: int) -> list[list]:
    """Splits a list into N roughly equal chunks."""
    try:
        chunk_size = len(items) // num_chunks
        chunks = [
            items[i * chunk_size:(i + 1) * chunk_size] if i < num_chunks - 1
            else items[i * chunk_size:]
            for i in range(num_chunks)
        ]
        logger.info(f"[split_into_chunks] {len(items)} items split into {num_chunks} chunks (~{chunk_size} each)")
        return chunks
    except Exception as e:
        logger.error(f"[split_into_chunks] Failed to split list: {e}")
        raise


def delete_existing_parquets(date_str: str) -> int:
    """
    Deletes all parquet files under the S3 prefix for a given date.
    Used to clean up stale files before reprocessing.
    Returns the number of deleted objects.
    """
    prefix = f"{S3_PREFIX}/exchange_date={date_str}/"

    try:
        s3       = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        objects  = response.get("Contents", [])

        if not objects:
            logger.info(f"[delete_existing_parquets] No existing files found at s3://{S3_BUCKET}/{prefix}")
            return 0

        delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3.delete_objects(Bucket=S3_BUCKET, Delete=delete_payload)

        logger.info(f"[delete_existing_parquets] Deleted {len(objects)} file(s) from s3://{S3_BUCKET}/{prefix}")
        return len(objects)

    except Exception as e:
        logger.error(f"[delete_existing_parquets] Failed to delete files at {prefix}: {e}")
        raise


def build_s3_key(date_str: str, chunk_id: int) -> str:
    """
    Builds the S3 key for a parquet chunk.
    Format: <S3_PREFIX>/<date_str>/<YYYYMMDD>_chunk_<id>.parquet
    Example: mastercard/exchange-rates/2026-02-01/20260201_chunk_1.parquet
    """
    file_date = datetime.strptime(date_str, DATE_FORMAT_INPUT).strftime(DATE_FORMAT_FILE)
    return f"{S3_PREFIX}/exchange_date={date_str}/{file_date}_chunk_{chunk_id}.parquet"


def save_chunk_to_s3(records: list[dict], date_str: str, chunk_id: int) -> str:
    """
    Serializes a list of exchange rate records into a parquet file and uploads it to S3.
    Skips records with missing fx_rate values.
    Returns the S3 key of the saved file.
    """
    s3_key        = build_s3_key(date_str, chunk_id)
    valid_records = [r for r in records if r["fx_rate"] != ""]
    skipped_count = len(records) - len(valid_records)

    if not valid_records:
        logger.warning(f"[save_chunk_to_s3] chunk_id={chunk_id} | No valid records to save, skipping upload")
        return s3_key

    try:
        table = pa.table({
            "date":          [r["date"]          for r in valid_records],
            "from_currency": [r["from_currency"] for r in valid_records],
            "to_currency":   [r["to_currency"]   for r in valid_records],
            "fx_rate":       [r["fx_rate"]       for r in valid_records],
        })
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | "
            f"written={len(valid_records)} | skipped={skipped_count} | "
            f"s3://{S3_BUCKET}/{s3_key}"
        )
        return s3_key

    except Exception as e:
        logger.error(f"[save_chunk_to_s3] chunk_id={chunk_id} | Failed to upload parquet: {e}")
        raise

# =============================================================================
# STEP 1: Fetch currency list from Mastercard
# =============================================================================

def fetch_currency_list() -> list[list[str]] | str:
    """
    Retrieves the full list of supported currencies from Mastercard
    and returns all valid currency pairs.
    """
    logger.info("[fetch_currency_list] Fetching supported currencies from Mastercard...")

    try:
        response = requests.Session().get(
            MASTERCARD_CURRENCIES_URL,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=True,
        )

        if response.status_code != 200:
            logger.error(f"[fetch_currency_list] Unexpected HTTP status: {response.status_code}")
            return "error"

        currencies = [c["alphaCd"] for c in response.json()["data"]["currencies"]]
        pairs      = [[src, dst] for src in currencies for dst in currencies if src != dst]

        logger.info(f"[fetch_currency_list] {len(currencies)} currencies -> {len(pairs)} pairs")
        return pairs

    except requests.exceptions.Timeout:
        logger.error("[fetch_currency_list] Request timed out")
        return "error"
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[fetch_currency_list] Connection error: {e}")
        return "error"
    except (KeyError, ValueError) as e:
        logger.error(f"[fetch_currency_list] Failed to parse response: {e}")
        return "error"
    except Exception as e:
        logger.error(f"[fetch_currency_list] Unexpected error: {e}")
        return "error"

# =============================================================================
# STEP 2: Fetch exchange rate for a single currency pair
# =============================================================================

def fetch_exchange_rate(
    date: str,
    pair: tuple[str, str],
    index: int,
    total: int,
) -> dict:
    """
    Fetches the exchange rate for a single currency pair on a given date.
    Returns a record dict with fx_rate="" if the request fails.
    """
    from_currency, to_currency = pair
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)

    params = {
        "exchange_date":               date_str,
        "transaction_currency":        from_currency,
        "cardholder_billing_currency": to_currency,
        "bank_fee":                    "0",
        "transaction_amount":          "1",
    }

    empty_record = {
        "date":          date_str,
        "from_currency": from_currency,
        "to_currency":   to_currency,
        "fx_rate":       "",
    }

    try:
        response = requests.get(
            MASTERCARD_RATES_URL,
            params=params,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
            verify=True,
        )

        if not response.text.strip():
            logger.warning(
                f"[{index}/{total}] Empty response (possible rate limit) | "
                f"{from_currency}->{to_currency} | {date_str}"
            )
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            return empty_record

        if response.status_code != 200:
            logger.warning(
                f"[{index}/{total}] HTTP {response.status_code} | "
                f"{from_currency}->{to_currency} | {date_str}"
            )
            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            return empty_record

        fx_rate = float(str(response.json()["data"]["conversionRate"]).replace(",", ""))
        time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

        logger.info(f"[{index}/{total}] OK {from_currency}->{to_currency} | {date_str} | fx={fx_rate}")
        return {
            "date":          date_str,
            "from_currency": from_currency,
            "to_currency":   to_currency,
            "fx_rate":       fx_rate,
        }

    except requests.exceptions.Timeout:
        logger.warning(f"[{index}/{total}] Timeout | {from_currency}->{to_currency} | {date_str}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[{index}/{total}] Connection error | {from_currency}->{to_currency} | {date_str} | {e}")
    except (KeyError, ValueError) as e:
        logger.error(f"[{index}/{total}] Failed to parse response | {from_currency}->{to_currency} | {date_str} | {e}")
    except Exception as e:
        logger.error(f"[{index}/{total}] Unexpected error | {from_currency}->{to_currency} | {date_str} | {e}")

    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
    return empty_record

# =============================================================================
# ORCHESTRATOR
# =============================================================================

def run_orchestrator(begin_date: str, end_date: str) -> dict:
    """
    Orchestrator role:
    - Fetches the full currency pair list
    - Deletes existing parquet files for each date before reprocessing
    - Splits pairs into chunks and invokes one worker Lambda per chunk
    """
    logger.info(f"[ORCHESTRATOR] Starting | begin={begin_date} | end={end_date}")

    try:
        dates = generate_date_range(begin_date, end_date)
        pairs = fetch_currency_list()

        if pairs == "error":
            raise RuntimeError("Failed to retrieve currency list from Mastercard")

        chunks        = split_into_chunks(pairs, NUM_CHUNKS)
        lambda_client = boto3.client("lambda")

        for date in dates:
            date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)

            # Clean up existing parquet files for this date before dispatching workers
            delete_existing_parquets(date_str)

            logger.info(f"[ORCHESTRATOR] Invoking {NUM_CHUNKS} workers for {date}...")

            for chunk_id, chunk in enumerate(chunks, start=1):
                try:
                    payload = {
                        "mode":     "worker",
                        "date":     date,
                        "pairs":    chunk,
                        "chunk_id": chunk_id,
                    }
                    response = lambda_client.invoke(
                        FunctionName=FUNCTION_NAME,
                        InvocationType="Event",
                        Payload=json.dumps(payload),
                    )
                    logger.info(
                        f"[ORCHESTRATOR] Worker {chunk_id} invoked | "
                        f"{len(chunk)} pairs | status={response['StatusCode']}"
                    )
                except Exception as e:
                    logger.error(f"[ORCHESTRATOR] Failed to invoke worker {chunk_id} for {date}: {e}")

        total_workers = NUM_CHUNKS * len(dates)
        logger.info(f"[ORCHESTRATOR] Done | {total_workers} workers launched | {len(pairs)} total pairs")

        return {
            "statusCode":  200,
            "mode":        "orchestrator",
            "workers":     total_workers,
            "total_pairs": len(pairs),
            "dates":       dates,
        }

    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Fatal error: {e}")
        raise

# =============================================================================
# WORKER
# =============================================================================

def run_worker(date: str, pairs: list, chunk_id: int) -> dict:
    """
    Worker role:
    - Fetches exchange rates for its assigned chunk of currency pairs
    - Saves results as a parquet file in S3
    """
    logger.info(f"[WORKER {chunk_id}] Starting | date={date} | pairs={len(pairs)}")

    try:
        date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
        total    = len(pairs)
        results  = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    fetch_exchange_rate,
                    date,
                    tuple(pair),
                    index + 1,
                    total,
                ): index
                for index, pair in enumerate(pairs)
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"[WORKER {chunk_id}] Error retrieving thread result: {e}")

        s3_key        = save_chunk_to_s3(results, date_str, chunk_id)
        written_count = len([r for r in results if r["fx_rate"] != ""])
        skipped_count = len(results) - written_count

        logger.info(
            f"[WORKER {chunk_id}] Done | date={date_str} | "
            f"written={written_count} | skipped={skipped_count} | file={s3_key}"
        )

        return {
            "statusCode":   200,
            "mode":         "worker",
            "chunk_id":     chunk_id,
            "records_ok":   written_count,
            "records_skip": skipped_count,
            "s3_key":       s3_key,
        }

    except Exception as e:
        logger.error(f"[WORKER {chunk_id}] Fatal error | date={date} | {e}")
        raise

# =============================================================================
# MAIN HANDLER
# =============================================================================

def lambda_handler(event: dict, context) -> dict:
    mode = event.get("mode", "orchestrator")
    logger.info(f"[lambda_handler] Event received | mode={mode}")

    try:
        if mode == "orchestrator":
            begin_date = event.get("begin_date", BEGIN_DATE)
            end_date   = event.get("end_date",   END_DATE)
            return run_orchestrator(begin_date, end_date)

        if mode == "worker":
            date     = event["date"]
            pairs    = event["pairs"]
            chunk_id = event.get("chunk_id", 0)
            return run_worker(date, pairs, chunk_id)

        raise ValueError(f"Unknown mode: '{mode}'. Use 'orchestrator' or 'worker'.")

    except KeyError as e:
        logger.error(f"[lambda_handler] Missing required field in event: {e}")
        raise
    except ValueError as e:
        logger.error(f"[lambda_handler] Invalid event value: {e}")
        raise
    except Exception as e:
        logger.error(f"[lambda_handler] Fatal error: {e}")
        raise