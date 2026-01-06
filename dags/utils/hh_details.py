import json
from pathlib import Path
import boto3
import os
import time
import requests
from typing import Dict, List, Tuple


BASE_URL = "https://api.hh.ru/vacancies"
HEADERS = {"User-Agent": "hh-remote-track/0.1 (aida.aitymova@gmail.com)"}


def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        endpoint_url=f'http://{os.getenv("MINIO_ENDPOINT")}',
        region_name="us-east-1",
    )


def load_vacancy_ids(ds: str, load_type: str) -> List[dict]:
    minio_bucket = os.getenv("MINIO_BUCKET")
    source_key = f"bronze/hh/vacancies_ids/load_type={load_type}/dt={ds}/part-000.jsonl"

    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=minio_bucket, Key=source_key)

    ids: List[dict] = []
    for line in response["Body"].iter_lines():
        if not line:
            continue
        row = json.loads(line.decode("utf-8"))
        ids.append(row)

    print(f"[load_vacancy_ids] loaded vacancy ids: {len(ids)} key={source_key}")
    return ids


def split_into_batches(rows: List[dict], batch_size: int = 200) -> List[List[dict]]:
    return [rows[i : i + batch_size] for i in range(0, len(rows), batch_size)]


def fetch_vacancy_details_batch(
    batch: List[dict],
    ds: str,
    load_type: str,
    batch_idx: int,
) -> Tuple[List[dict], List[str], Dict[int, int], int]:
    details_rows: List[dict] = []
    failed_ids: List[str] = []
    status_counts: Dict[int, int] = {}
    exception_count = 0

    session = requests.Session()

    for row in batch:
        vacancy_id = row.get("vacancy_id")
        if not vacancy_id:
            failed_ids.append(str(vacancy_id))
            status_counts[0] = status_counts.get(0, 0) + 1  # 0 = bad input
            continue

        url = f"{BASE_URL}/{vacancy_id}"

        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            time.sleep(0.3)

            # Важно: статус != 200 — это не exception, но это FAIL по бизнес-логике
            if resp.status_code != 200:
                status_counts[resp.status_code] = status_counts.get(resp.status_code, 0) + 1
                failed_ids.append(str(vacancy_id))
                continue

            detail = resp.json()

        except requests.exceptions.RequestException:
            exception_count += 1
            failed_ids.append(str(vacancy_id))
            continue
        except ValueError:
            # json decode error
            exception_count += 1
            failed_ids.append(str(vacancy_id))
            continue

        # ✅ делаем self-contained record: добавляем метаданные из manifest
        detail["search_profile"] = row.get("search_profile")
        detail["expected_risk_category"] = row.get("expected_risk_category")
        detail["load_dt"] = ds
        detail["load_type"] = load_type
        detail["batch_idx"] = batch_idx

        details_rows.append(detail)

    return details_rows, failed_ids, status_counts, exception_count


def save_details_batch_to_minio(
    details_rows: List[dict], ds: str, load_type: str, batch_idx: int
) -> str:
    local_path = f"/tmp/vacancy_details_{ds}_batch_{batch_idx:03d}.jsonl"
    Path("/tmp").mkdir(parents=True, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        for item in details_rows:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()

    base_prefix = "bronze/hh/vacancy_details"
    object_key = f"{base_prefix}/load_type={load_type}/dt={ds}/batch={batch_idx:03d}/part-000.jsonl"

    s3_client.upload_file(local_path, minio_bucket, object_key)
    return f"s3://{minio_bucket}/{object_key}"


def build_details_coverage_report(ds: str, load_type: str):
    expected_rows = load_vacancy_ids(ds, load_type)
    expected_set = {row["vacancy_id"] for row in expected_rows if row.get("vacancy_id")}
    expected_count = len(expected_set)

    prefix = f"bronze/hh/vacancy_details/load_type={load_type}/dt={ds}/"
    minio_bucket = os.getenv("MINIO_BUCKET")
    s3_client = get_s3_client()

    resp = s3_client.list_objects_v2(Bucket=minio_bucket, Prefix=prefix)
    contents = resp.get("Contents", [])
    keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".jsonl")]

    loaded_set = set()
    for key in keys:
        obj = s3_client.get_object(Bucket=minio_bucket, Key=key)
        for line in obj["Body"].iter_lines():
            if not line:
                continue
            row = json.loads(line.decode("utf-8"))
            # В vacancy_details id хранится как "id"
            if row.get("id"):
                loaded_set.add(str(row["id"]))

    loaded_count = len(loaded_set)
    missing_set = {str(x) for x in expected_set} - loaded_set
    missing_count = len(missing_set)
    coverage_pct = 0.0 if expected_count == 0 else loaded_count / expected_count * 100

    severity = (
        "OK"
        if missing_count == 0
        else ("WARNING" if (missing_count <= 5 or coverage_pct >= 99) else "CRITICAL")
    )

    report = {
        "ds": ds,
        "load_type": load_type,
        "expected_count": expected_count,
        "loaded_count": loaded_count,
        "missing_count": missing_count,
        "coverage_pct": coverage_pct,
        "found_files": len(keys),
        "sample_missing_ids": list(sorted(missing_set)[:5]),
        "severity": severity,
    }

    object_key = (
        f"bronze/hh/reports/vacancy_details_coverage/load_type={load_type}/dt={ds}/report.json"
    )

    local_path = f"/tmp/vacancy_details_coverage_{ds}_{load_type}.json"
    Path("/tmp").mkdir(parents=True, exist_ok=True)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    s3_client.upload_file(local_path, minio_bucket, object_key)

    s3_path = f"s3://{minio_bucket}/{object_key}"
    print(f"[coverage_report] {json.dumps(report, ensure_ascii=False)}")
    print(f"[coverage_report] saved: {s3_path}")
    return s3_path


def collect_vacancy_details(ds: str, load_type: str, batch_size: int = 200):
    expected_ids = load_vacancy_ids(ds, load_type)
    batches = split_into_batches(expected_ids, batch_size=batch_size)

    total_ok = 0
    total_failed = 0
    total_exceptions = 0
    total_status_counts: Dict[int, int] = {}

    for batch_idx, batch in enumerate(batches):
        details_rows, failed_ids, status_counts, exception_count = fetch_vacancy_details_batch(
            batch=batch,
            ds=ds,
            load_type=load_type,
            batch_idx=batch_idx,
        )

        save_details_batch_to_minio(details_rows, ds, load_type, batch_idx)

        ok_count = len(details_rows)
        failed_count = len(failed_ids)

        total_ok += ok_count
        total_failed += failed_count
        total_exceptions += exception_count

        for k, v in status_counts.items():
            total_status_counts[k] = total_status_counts.get(k, 0) + v

        # ✅ один лог на батч (не печатаем огромные списки)
        print(
            f"[batch {batch_idx:03d}] ok={ok_count} failed={failed_count} "
            f"status_counts={status_counts} exceptions={exception_count}"
        )

    print(
        f"[TOTAL] expected={len(expected_ids)} batches={len(batches)} "
        f"ok={total_ok} failed={total_failed} "
        f"status_counts_total={total_status_counts} exceptions_total={total_exceptions}"
    )
