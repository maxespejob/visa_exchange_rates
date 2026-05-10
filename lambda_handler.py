# =============================================================================
# AWS LAMBDA - VISA Exchange Rates
# Patrón Fan-out: una sola función, dos roles
#
# Modo ORCHESTRATOR:
#   - Obtiene lista de monedas
#   - Divide los pares en chunks
#   - Invoca N workers en paralelo (async)
#   - Cada worker guarda su resultado en S3
#
# Modo WORKER:
#   - Recibe un chunk de pares
#   - Scrapea con Playwright
#   - Guarda JSON en S3
#
# VARIABLES DE ENTORNO:
#   S3_BUCKET      → nombre del bucket
#   S3_PREFIX      → prefijo (ej: raw/visa/)
#   FUNCTION_NAME  → nombre de esta misma función Lambda (para auto-invocarse)
#
# EVENTOS DE ENTRADA:
#   Orquestadora: {"mode": "orchestrator", "begin_date": "2025-07-01", "end_date": "2025-07-01"}
#   Worker:       {"mode": "worker", "fecha": "07/01/2025", "pairs": [...], "chunk_id": 1}
#
# PERMISOS IAM ADICIONALES NECESARIOS:
#   lambda:InvokeFunction sobre esta misma función
# =============================================================================

import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

EXCHANGE_TABLE = os.environ.get("EXCHANGE_TABLE", "visa_exchange_rates")
FUNCTION_NAME = os.environ.get("FUNCTION_NAME", "visa-exchange-rates-scraper")
BEGIN_DATE    = os.environ.get("BEGIN_DATE",    datetime.utcnow().strftime("%Y-%m-%d"))
END_DATE      = os.environ.get("END_DATE",      datetime.utcnow().strftime("%Y-%m-%d"))

CONCURRENCIA  = 8
TIMEOUT_MS    = 12000
NUM_CHUNKS    = 12       # workers en paralelo → 28056 / 6 ≈ 4676 pares c/u

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36",
]

EXTRA_HEADERS = {
    "Referer":         "https://www.visa.com.pe/",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}

VISA_CALCULATOR_URL = (
    "https://www.visa.com.pe/soporte/consumidores/viajes/"
    "exchange-rate-calculator.html"
)
VISA_RATES_URL = (
    "https://www.visa.com.pe/cmsapi/fx/rates?"
    "amount=1&fee=0"
    "&utcConvertedDate={fecha}"
    "&exchangedate={fecha}"
    "&fromCurr={to_curr}"
    "&toCurr={from_curr}"
)


# =============================================================================
# HELPERS COMPARTIDOS
# =============================================================================

def generate_dates_range(begin_date_str: str, end_date_str: str) -> list[str]:
    begin = datetime.strptime(begin_date_str, "%Y-%m-%d")
    end   = datetime.strptime(end_date_str,   "%Y-%m-%d")
    return [
        (begin + timedelta(days=i)).strftime("%m/%d/%Y")
        for i in range((end - begin).days + 1)
    ]


def get_visa_currency_list() -> list[list[str]]:
    import urllib.request
    with urllib.request.urlopen(VISA_CALCULATOR_URL, timeout=15) as resp:
        html = resp.read().decode("utf-8")

    body       = BeautifulSoup(html, "html.parser")
    calculator = body.find("dm-calculator")
    data       = json.loads(calculator.get("content"))

    keys = [c["key"] for c in data["currencyList"] if c["key"] != "None"]
    keys.append("SLE")

    pairs = [[c1, c2] for c1 in keys for c2 in keys if c1 != c2]
    print(f"[get_visa_currency_list] {len(keys)} monedas → {len(pairs)} pares")
    return pairs


def split_into_chunks(lst: list, n: int) -> list[list]:
    size = len(lst) // n
    return [lst[i * size:(i + 1) * size] if i < n - 1 else lst[i * size:] for i in range(n)]

def save_to_dynamodb(records: list[dict]) -> tuple[int, int]:
    dynamodb = boto3.resource("dynamodb")
    table    = dynamodb.Table(EXCHANGE_TABLE)

    written = 0
    skipped = 0

    with table.batch_writer() as batch:
        for r in records:
            if r["fxRate"] == "":
                skipped += 1
                continue
            batch.put_item(Item={
                "date":          r["date"],
                "currency_pair": f"{r['fromCurrency']}#{r['toCurrency']}",
                "fx_rate":       Decimal(str(r["fxRate"])),
            })
            written += 1

    print(f"[save_to_dynamodb] ✅ escritos={written} | omitidos={skipped}")
    return written, skipped
# =============================================================================
# ROL ORQUESTADORA
# =============================================================================

def run_orchestrator(begin_date: str, end_date: str):
    print(f"[ORCHESTRATOR] Iniciando | begin={begin_date} | end={end_date}")

    dates  = generate_dates_range(begin_date, end_date)
    pairs  = get_visa_currency_list()
    chunks = split_into_chunks(pairs, NUM_CHUNKS)

    lambda_client = boto3.client("lambda")

    for fecha in dates:
        print(f"[ORCHESTRATOR] Invocando {NUM_CHUNKS} workers para {fecha}...")

        for chunk_id, chunk in enumerate(chunks, start=1):
            payload = {
                "mode":     "worker",
                "fecha":    fecha,
                "pairs":    chunk,
                "chunk_id": chunk_id,
            }
            response = lambda_client.invoke(
                FunctionName=FUNCTION_NAME,
                InvocationType="Event",   # async — no espera respuesta
                Payload=json.dumps(payload),
            )
            print(f"[ORCHESTRATOR] Worker {chunk_id} invocado | {len(chunk)} pares | status={response['StatusCode']}")

    print(f"[ORCHESTRATOR] ✅ {NUM_CHUNKS * len(dates)} workers lanzados")
    return {
        "statusCode":  200,
        "mode":        "orchestrator",
        "workers":     NUM_CHUNKS * len(dates),
        "total_pairs": len(pairs),
        "dates":       dates,
    }


# =============================================================================
# ROL WORKER
# =============================================================================

async def scrape_chunk(fecha: str, pairs: list) -> list[dict]:
    from playwright.async_api import async_playwright  # import local — no afecta init

    results = []
    queue   = asyncio.Queue()
    total   = len(pairs)

    for idx, pair in enumerate(pairs, 1):
        await queue.put((idx, pair))

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
                "--no-zygote",
            ],
        )

        # Un solo contexto compartido — múltiples contextos con --single-process crashea Chromium
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            extra_http_headers=EXTRA_HEADERS,
        )

        async def worker(worker_id: int):
            page = await context.new_page()

            while True:
                try:
                    idx, pair = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                from_curr, to_curr = pair
                formatted_date     = datetime.strptime(fecha, "%m/%d/%Y").strftime("%Y-%m-%d")
                url                = VISA_RATES_URL.format(
                    fecha=fecha, from_curr=from_curr, to_curr=to_curr
                )

                try:
                    await page.goto(url, wait_until="load", timeout=TIMEOUT_MS)
                    raw  = await page.inner_text("pre")
                    data = json.loads(raw)
                    fx   = data["originalValues"]["fxRateVisa"]
                    results.append({"date": formatted_date, "fromCurrency": from_curr, "toCurrency": to_curr, "fxRate": fx})
                    print(f"[W{worker_id}] ✅ [{idx}/{total}] {from_curr}->{to_curr}")

                except Exception as e:
                    results.append({"date": formatted_date, "fromCurrency": from_curr, "toCurrency": to_curr, "fxRate": ""})
                    print(f"[W{worker_id}] ❌ [{idx}/{total}] {from_curr}->{to_curr} | {e}")

                await asyncio.sleep(random.uniform(0.9, 1.3))

            await page.close()

        workers = [asyncio.create_task(worker(i)) for i in range(CONCURRENCIA)]
        await asyncio.gather(*workers)
        await browser.close()

    print(f"[scrape_chunk] ✅ {len(results)} registros extraídos")
    return results


def run_worker(fecha: str, pairs: list, chunk_id: int):
    print(f"[WORKER {chunk_id}] Iniciando | fecha={fecha} | pares={len(pairs)}")

    records = asyncio.run(scrape_chunk(fecha, pairs))

    #s3  = boto3.client("s3")
    written, skipped = save_to_dynamodb(records)
    
    #written = 0
    #skipped = 0
    
    print(f"[WORKER {chunk_id}] ✅ fecha={fecha} | escritos={written} | omitidos={skipped}")
    return {
        "statusCode":   200,
        "mode":         "worker",
        "chunk_id":     chunk_id,
        "records_ok":   written,
        "records_skip": skipped,
    }
    
    #ts  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    #key = f"{S3_PREFIX.rstrip('/')}/{fecha}chunk_{chunk_id}_{fecha.replace('/', '-')}_{ts}.json"

    # s3.put_object(
    #     Bucket=S3_BUCKET,
    #     Key=key,
    #     Body=json.dumps(records, ensure_ascii=False),
    #     ContentType="application/json",
    # )

    # print(f"[WORKER {chunk_id}] ✅ Guardado en s3://{S3_BUCKET}/{key}/{fecha}")
    # return {
    #     "statusCode": 200,
    #     "mode":       "worker",
    #     "chunk_id":   chunk_id,
    #     "records":    len(records),
    #     "s3_key":     key,
    # }


# =============================================================================
# HANDLER PRINCIPAL — decide el rol según el evento
# =============================================================================

def lambda_handler(event: dict, context):
    mode = event.get("mode", "orchestrator")
    ### En realidad, siempre utiliza el orchestrator, que delegará tareas de worker"
    if mode == "orchestrator":
        begin_date = event.get("begin_date", BEGIN_DATE)
        end_date   = event.get("end_date",   END_DATE)
        return run_orchestrator(begin_date, end_date)

    elif mode == "worker":
        fecha    = event["fecha"]
        pairs    = event["pairs"]
        chunk_id = event.get("chunk_id", 0)
        return run_worker(fecha, pairs, chunk_id)

    else:
        raise ValueError(f"Modo desconocido: {mode}. Usar 'orchestrator' o 'worker'.")