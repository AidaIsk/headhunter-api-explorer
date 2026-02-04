import json
import os
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

import boto3
import requests


BASE_URL = "https://api.hh.ru/vacancies"
HEADERS = {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}

# Политика ретраев
MAX_RETRIES = 3
BASE_SLEEP_SEC = 0.3  # базовая задержка между запросами
BACKOFF_SEC = 2.0     # множитель backoff для ретраев


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        endpoint_url=f'http://{os.getenv("MINIO_ENDPOINT")}',
        region_name="us-east-1",
    )


def _dump_jsonl(local_path: str, rows: List[dict]) -> None:
    Path(Path(local_path).parent).mkdir(parents=True, exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        for item in rows:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _upload_file_to_minio(local_path: str, object_key: str) -> str:
    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()
    s3_client.upload_file(local_path, minio_bucket, object_key)
    return f"s3://{minio_bucket}/{object_key}"


def load_vacancy_ids(ds: str, load_type: str) -> List[dict]:
    minio_bucket = os.getenv("MINIO_BUCKET")
    source_key = f"bronze/hh/vacancies_ids/load_type={load_type}/dt={ds}/part-000.jsonl"

    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=minio_bucket, Key=source_key)

    rows: List[dict] = []
    for line in response["Body"].iter_lines():
        if not line:
            continue
        rows.append(json.loads(line.decode("utf-8")))

    print(f"[load_vacancy_ids] loaded={len(rows)} key={source_key}")
    return rows


def split_into_batches(rows: List[dict], batch_size: int = 200) -> List[List[dict]]:
    return [rows[i: i + batch_size] for i in range(0, len(rows), batch_size)]


def _classify_http_reason(status_code: int) -> str:
    if status_code == 404:
        return "not_found"
    if status_code == 429:
        return "rate_limited"
    if 500 <= status_code <= 599:
        return "server_error"
    if status_code == 403:
        return "forbidden"
    return "http_error"


def _request_with_retries(
    session: requests.Session,
    url: str,
    headers: dict,
    timeout: int,
) -> Tuple[Optional[requests.Response], Optional[str]]:
    """
    Возвращает:
      (response, error_type)
    где error_type None если success (HTTP 200),
    иначе 'http_error'|'request_exception'|'json_decode_error'.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)

            # Небольшая пауза между запросами (бережём API)
            time.sleep(BASE_SLEEP_SEC)

            # Успех
            if resp.status_code == 200:
                return resp, None

            # Для 429/5xx делаем retry с backoff
            if resp.status_code == 429 or (500 <= resp.status_code <= 599):
                if attempt < MAX_RETRIES:
                    sleep_s = BACKOFF_SEC * attempt
                    time.sleep(sleep_s)
                    continue

            # Для остальных статусов — без ретрая
            return resp, "http_error"

        except requests.exceptions.RequestException:
            # сетевые/таймауты — retry
            if attempt < MAX_RETRIES:
                sleep_s = BACKOFF_SEC * attempt
                time.sleep(sleep_s)
                continue
            return None, "request_exception"

    return None, "request_exception"


def fetch_vacancy_details_batch(
    batch: List[dict],
    ds: str,
    load_type: str,
    batch_idx: int,
) -> Tuple[List[dict], List[dict], Dict[int, int], int]:
    """
    Returns:
      details_rows: успешные детали вакансий (self-contained)
      failed_records: записи по failed (с причиной)
      status_counts: счётчик HTTP статусов (для http_error кейсов)
      exception_count: сколько раз упали request_exception/json_decode_error
    """
    details_rows: List[dict] = []
    failed_records: List[dict] = []
    status_counts: Dict[int, int] = {}
    exception_count = 0

    session = requests.Session()

    for row in batch:
        vacancy_id = row.get("vacancy_id")
        if not vacancy_id:
            failed_records.append(
                {
                    "vacancy_id": None,
                    "http_status": 0,
                    "reason": "bad_input",
                    "ds": ds,
                    "load_type": load_type,
                    "batch_idx": batch_idx,
                }
            )
            status_counts[0] = status_counts.get(0, 0) + 1
            continue

        url = f"{BASE_URL}/{vacancy_id}"
        resp, err_type = _request_with_retries(session, url, HEADERS, timeout=30)

        # HTTP != 200 или request_exception
        if err_type is not None:
            if resp is not None:
                status = resp.status_code
                status_counts[status] = status_counts.get(status, 0) + 1
                reason = _classify_http_reason(status) if err_type == "http_error" else err_type
                failed_records.append(
                    {
                        "vacancy_id": str(vacancy_id),
                        "http_status": status,
                        "reason": reason,
                        "ds": ds,
                        "load_type": load_type,
                        "batch_idx": batch_idx,
                    }
                )
            else:
                exception_count += 1
                failed_records.append(
                    {
                        "vacancy_id": str(vacancy_id),
                        "http_status": "exception",
                        "reason": err_type,
                        "ds": ds,
                        "load_type": load_type,
                        "batch_idx": batch_idx,
                    }
                )
            continue

        # 200: парсим json
        try:
            detail: Dict[str, Any] = resp.json()
        except ValueError:
            exception_count += 1
            failed_records.append(
                {
                    "vacancy_id": str(vacancy_id),
                    "http_status": 200,
                    "reason": "json_decode_error",
                    "ds": ds,
                    "load_type": load_type,
                    "batch_idx": batch_idx,
                }
            )
            continue

        # self-contained record: добавляем метаданные из manifest
        detail["search_profile"] = row.get("search_profile")
        detail["expected_risk_category"] = row.get("expected_risk_category")
        detail["load_dt"] = ds
        detail["load_type"] = load_type
        detail["batch_idx"] = batch_idx

        details_rows.append(detail)

    return details_rows, failed_records, status_counts, exception_count


def save_details_batch_to_minio(details_rows: List[dict], ds: str, load_type: str, batch_idx: int) -> str:
    local_path = f"/tmp/vacancy_details_{ds}_batch_{batch_idx:03d}.jsonl"
    _dump_jsonl(local_path, details_rows)

    object_key = f"bronze/hh/vacancy_details/load_type={load_type}/dt={ds}/batch={batch_idx:03d}/part-000.jsonl"
    return _upload_file_to_minio(local_path, object_key)


def save_failed_records_to_minio(failed_records: List[dict], ds: str, load_type: str, batch_idx: int) -> Optional[str]:
    if not failed_records:
        return None

    local_path = f"/tmp/vacancy_details_failures_{ds}_batch_{batch_idx:03d}.jsonl"
    _dump_jsonl(local_path, failed_records)

    object_key = (
        f"bronze/hh/vacancy_details_failures/load_type={load_type}/dt={ds}/batch={batch_idx:03d}/part-000.jsonl"
    )
    return _upload_file_to_minio(local_path, object_key)


def save_failed_records_daily_to_minio(failed_records: List[dict], ds: str, load_type: str) -> Optional[str]:
    """
    Один общий файл failures за день — удобно для анализа.
    """
    if not failed_records:
        return None

    local_path = f"/tmp/vacancy_details_failures_{ds}_{load_type}.jsonl"
    _dump_jsonl(local_path, failed_records)

    object_key = f"bronze/hh/vacancy_details_failures/load_type={load_type}/dt={ds}/failures.jsonl"
    return _upload_file_to_minio(local_path, object_key)


def _list_all_keys(bucket: str, prefix: str) -> List[str]:
    """
    list_objects_v2 с пагинацией — чтобы не пропускать файлы.
    """
    s3_client = get_s3_client()
    keys: List[str] = []
    token: Optional[str] = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])
        keys.extend([obj["Key"] for obj in contents])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def build_details_coverage_report(ds: str, load_type: str, **context) -> str:
    """
    Coverage:
      expected = все vacancy_id из manifest
      loaded   = все id из vacancy_details
      also:
        failed_total_by_reason — сколько упало и почему (по failures файлам)
    """

    ti = context["ti"]

    expected_rows = load_vacancy_ids(ds, load_type)
    expected_set = {str(row["vacancy_id"]) for row in expected_rows if row.get("vacancy_id")}
    expected_count = len(expected_set)

    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()

    # --- loaded (details)
    details_prefix = f"bronze/hh/vacancy_details/load_type={load_type}/dt={ds}/"
    detail_keys = [k for k in _list_all_keys(minio_bucket, details_prefix) if k.endswith(".jsonl")]

    loaded_set = set()
    for key in detail_keys:
        obj = s3_client.get_object(Bucket=minio_bucket, Key=key)
        for line in obj["Body"].iter_lines():
            if not line:
                continue
            row = json.loads(line.decode("utf-8"))
            if row.get("id"):
                loaded_set.add(str(row["id"]))

    loaded_count = len(loaded_set)
    missing_set = expected_set - loaded_set
    missing_count = len(missing_set)
    coverage_pct = 0.0 if expected_count == 0 else loaded_count / expected_count * 100

    # --- failures breakdown (optional, но полезно)
    failures_prefix = f"bronze/hh/vacancy_details_failures/load_type={load_type}/dt={ds}/"
    failure_keys = [k for k in _list_all_keys(minio_bucket, failures_prefix) if k.endswith(".jsonl")]

    failures_by_reason: Dict[str, int] = {}
    failures_by_status: Dict[str, int] = {}

    for key in failure_keys:
        obj = s3_client.get_object(Bucket=minio_bucket, Key=key)
        for line in obj["Body"].iter_lines():
            if not line:
                continue
            r = json.loads(line.decode("utf-8"))
            reason = str(r.get("reason", "unknown"))
            status = str(r.get("http_status", "unknown"))
            failures_by_reason[reason] = failures_by_reason.get(reason, 0) + 1
            failures_by_status[status] = failures_by_status.get(status, 0) + 1

    # severity логика:
    # - OK: missing=0
    # - WARNING: missing небольшие ИЛИ почти 100%
    # - CRITICAL: иначе
    severity = (
        "OK"
        if missing_count == 0
        else ("WARNING" if (missing_count <= 5 or coverage_pct >= 99) else "CRITICAL")
    )

    ti.xcom_push(
        key="severity",
        value=severity
    )

    report = {
        "ds": ds,
        "load_type": load_type,
        "expected_count": expected_count,
        "loaded_count": loaded_count,
        "missing_count": missing_count,
        "coverage_pct": coverage_pct,
        "found_files": len(detail_keys),
        "sample_missing_ids": list(sorted(missing_set)[:5]),
        "severity": severity,
        "failures_files": len(failure_keys),
        "failures_by_reason": failures_by_reason,
        "failures_by_status": failures_by_status,
    }

    object_key = f"bronze/hh/reports/vacancy_details_coverage/load_type={load_type}/dt={ds}/report.json"
    local_path = f"/tmp/vacancy_details_coverage_{ds}_{load_type}.json"
    Path("/tmp").mkdir(parents=True, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    s3_client.upload_file(local_path, minio_bucket, object_key)

    s3_path = f"s3://{minio_bucket}/{object_key}"
    print(f"[coverage_report] {json.dumps(report, ensure_ascii=False)}")
    print(f"[coverage_report] saved: {s3_path}")
    return s3_path

def collect_vacancy_details(ds: str, load_type: str, batch_size: int = 200) -> None:
    expected_rows = load_vacancy_ids(ds, load_type)
    batches = split_into_batches(expected_rows, batch_size=batch_size)

    total_ok = 0
    total_failed = 0
    total_exceptions = 0
    total_status_counts: Dict[int, int] = {}
    all_failed_records: List[dict] = []

    for batch_idx, batch in enumerate(batches):
        details_rows, failed_records, status_counts, exception_count = fetch_vacancy_details_batch(
            batch=batch,
            ds=ds,
            load_type=load_type,
            batch_idx=batch_idx,
        )

        save_details_batch_to_minio(details_rows, ds, load_type, batch_idx)
        failed_path = save_failed_records_to_minio(failed_records, ds, load_type, batch_idx)

        ok_count = len(details_rows)
        failed_count = len(failed_records)

        total_ok += ok_count
        total_failed += failed_count
        total_exceptions += exception_count
        all_failed_records.extend(failed_records)

        for k, v in status_counts.items():
            total_status_counts[k] = total_status_counts.get(k, 0) + v

        print(
            f"[batch {batch_idx:03d}] ok={ok_count} failed={failed_count} "
            f"status_counts={status_counts} exceptions={exception_count} "
            f"failed_saved_to={failed_path}"
        )

    daily_failed_path = save_failed_records_daily_to_minio(all_failed_records, ds, load_type)

    print(
        f"[TOTAL] expected={len(expected_rows)} batches={len(batches)} "
        f"ok={total_ok} failed={total_failed} "
        f"status_counts_total={total_status_counts} exceptions_total={total_exceptions} "
        f"daily_failures_saved_to={daily_failed_path}"
    )
